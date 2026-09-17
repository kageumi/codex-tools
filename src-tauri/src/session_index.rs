use crate::{
    models::{AppError, PageResult, SessionSummary},
    provider_sync,
};
use serde_json::Value;
use std::{
    borrow::Cow,
    collections::HashMap,
    fs,
    io::{BufRead, Read},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime},
};

const CACHE_RECHECK_INTERVAL: Duration = Duration::from_secs(1);
const MAX_ROLLOUT_INDEX_BYTES: u64 = 2 * 1024 * 1024;
const MAX_ROLLOUT_LINE_BYTES: usize = 256 * 1024;
const MAX_SESSION_ID_CHARS: usize = 512;
const MAX_SESSION_PATH_CHARS: usize = 4_096;

#[derive(Clone, Default)]
pub struct SessionIndex {
    cache: Arc<RwLock<Option<CachedIndex>>>,
}

struct CachedIndex {
    home: PathBuf,
    sources: Vec<SourceStamp>,
    sessions: Arc<Vec<SessionSummary>>,
    database_count: usize,
    checked_at: Instant,
}

#[derive(Clone, PartialEq, Eq)]
struct SourceStamp {
    path: PathBuf,
    length: u64,
    modified: Option<SystemTime>,
}

impl SessionIndex {
    pub fn load_recent(&self, codex_home: &Path) -> anyhow::Result<Arc<Vec<SessionSummary>>> {
        if let Some(sessions) = self
            .cache
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .filter(|cache| {
                cache.home == codex_home && cache.checked_at.elapsed() < CACHE_RECHECK_INTERVAL
            })
            .map(|cache| cache.sessions.clone())
        {
            return Ok(sessions);
        }
        self.load(codex_home)
    }

    pub fn load(&self, codex_home: &Path) -> anyhow::Result<Arc<Vec<SessionSummary>>> {
        Ok(self.load_with_database_count(codex_home)?.0)
    }

    pub fn load_with_database_count(
        &self,
        codex_home: &Path,
    ) -> anyhow::Result<(Arc<Vec<SessionSummary>>, usize)> {
        let (database_paths, rollout_paths, sources) = session_sources(codex_home);
        let checked_at = Instant::now();
        if let Some((sessions, database_count)) = self
            .cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
            .filter(|cache| cache.home == codex_home && cache.sources == sources)
            .map(|cache| {
                cache.checked_at = checked_at;
                (cache.sessions.clone(), cache.database_count)
            })
        {
            return Ok((sessions, database_count));
        }

        let sessions = Arc::new(rebuild_from_paths(&database_paths, &rollout_paths)?);
        let database_count = database_paths.len();
        let result = (sessions.clone(), database_count);
        *self
            .cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(CachedIndex {
            home: codex_home.to_path_buf(),
            sources,
            sessions,
            database_count,
            checked_at,
        });
        Ok(result)
    }

    pub fn invalidate(&self) {
        *self
            .cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    /// 只刷新本次修复实际改动过的来源（rollout / 数据库），其余会话保持不变。
    /// 用于切换账号或服务后替代全量重建：缓存未初始化、home 不匹配或没有
    /// 受影响路径时直接返回；其它来源也发生变化时使缓存失效。
    pub fn refresh_paths(&self, codex_home: &Path, affected: &[PathBuf]) -> anyhow::Result<()> {
        if affected.is_empty() {
            return Ok(());
        }
        let (database_paths, rollout_paths, sources) = session_sources(codex_home);
        let checked_at = Instant::now();
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(cached) = cache.as_mut() else {
            return Ok(());
        };
        if cached.home != codex_home {
            return Ok(());
        }
        let mut affected_dbs: Vec<PathBuf> = Vec::new();
        let mut affected_rollouts: Vec<PathBuf> = Vec::new();
        for path in affected {
            if is_sqlite_source(path) {
                affected_dbs.push(path.clone());
            } else if is_rollout_source(path) {
                affected_rollouts.push(path.clone());
            }
        }
        affected_dbs.sort();
        affected_rollouts.sort();
        let covered: std::collections::HashSet<PathBuf> = affected
            .iter()
            .flat_map(|path| [path.clone(), sqlite_sidecar(path, "-wal")])
            .collect();
        let previous: HashMap<&Path, &SourceStamp> = cached
            .sources
            .iter()
            .map(|stamp| (stamp.path.as_path(), stamp))
            .collect();
        let current: HashMap<&Path, &SourceStamp> = sources
            .iter()
            .map(|stamp| (stamp.path.as_path(), stamp))
            .collect();
        let outside_changed = previous.iter().any(|(path, old)| {
            if covered.contains(*path) {
                return false;
            }
            match current.get(path) {
                Some(new) => *old != *new,
                None => true,
            }
        }) || current
            .iter()
            .any(|(path, _)| !covered.contains(*path) && !previous.contains_key(path));
        if outside_changed {
            *cache = None;
            return Ok(());
        }
        // 调用方明确声明这些来源已变更；即使文件系统时间戳粒度不足以区分
        // 同长度重写，也必须重新读取并合并受影响来源。
        if !affected_dbs.is_empty() || !affected_rollouts.is_empty() {
            let sessions = merge_refreshed_sessions(
                &cached.sessions,
                &affected_dbs,
                &affected_rollouts,
                &database_paths,
                &rollout_paths,
            )?;
            cached.sessions = Arc::new(sessions);
        }
        cached.sources = sources;
        cached.database_count = database_paths.len();
        cached.checked_at = checked_at;
        Ok(())
    }
}

fn is_rollout_source(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "jsonl")
}

fn is_sqlite_source(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "sqlite" || extension == "db")
}

/// 把受影响来源重建出的会话合并进现有索引：只有当某条会话的权威来源（最后
/// 写入 provider 的来源，rollout 优先于数据库）确实属于受影响路径时才整体替换，
/// 否则保留现有值，避免未受影响来源覆盖的 provider 被回退。
fn merge_refreshed_sessions(
    existing: &[SessionSummary],
    affected_dbs: &[PathBuf],
    affected_rollouts: &[PathBuf],
    all_databases: &[PathBuf],
    all_rollouts: &[PathBuf],
) -> anyhow::Result<Vec<SessionSummary>> {
    let scope = provider_sync::session_scope(all_databases, all_rollouts)?;
    let affected = rebuild_from_paths_with_scope(affected_dbs, affected_rollouts, &scope)?;
    let affected_rollout_set: std::collections::HashSet<&Path> =
        affected_rollouts.iter().map(PathBuf::as_path).collect();
    let affected_db_set: std::collections::HashSet<&Path> =
        affected_dbs.iter().map(PathBuf::as_path).collect();
    let mut by_id: HashMap<String, SessionSummary> = existing
        .iter()
        .map(|session| (session.id.clone(), session.clone()))
        .collect();
    for session in affected {
        match by_id.get_mut(&session.id) {
            Some(current) => {
                if authoritative_source_affected(current, &affected_db_set, &affected_rollout_set) {
                    *current = session;
                }
            }
            None => {
                by_id.insert(session.id.clone(), session);
            }
        }
    }
    let mut output: Vec<SessionSummary> = by_id.into_values().collect();
    output.sort_by_key(|session| std::cmp::Reverse(session.updated_at));
    Ok(output)
}

fn authoritative_source_affected(
    session: &SessionSummary,
    affected_dbs: &std::collections::HashSet<&Path>,
    affected_rollouts: &std::collections::HashSet<&Path>,
) -> bool {
    match session.source_rollout.as_deref() {
        Some(path) => affected_rollouts.contains(Path::new(path)),
        None => affected_dbs.contains(Path::new(&session.source_db)),
    }
}

fn session_sources(codex_home: &Path) -> (Vec<PathBuf>, Vec<PathBuf>, Vec<SourceStamp>) {
    let database_paths = provider_sync::database_paths(codex_home);
    let mut rollout_paths = provider_sync::rollout_files(codex_home);
    rollout_paths.sort();
    let sources = database_paths
        .iter()
        .flat_map(|path| [path.clone(), sqlite_sidecar(path, "-wal")])
        .chain(rollout_paths.iter().cloned())
        .map(|path| {
            let metadata = fs::metadata(&path).ok();
            SourceStamp {
                path,
                length: metadata.as_ref().map(fs::Metadata::len).unwrap_or(0),
                modified: metadata.and_then(|value| value.modified().ok()),
            }
        })
        .collect();
    (database_paths, rollout_paths, sources)
}

fn sqlite_sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn rebuild_from_paths(
    database_paths: &[PathBuf],
    rollout_paths: &[PathBuf],
) -> anyhow::Result<Vec<SessionSummary>> {
    let scope = provider_sync::session_scope(database_paths, rollout_paths)?;
    rebuild_from_paths_with_scope(database_paths, rollout_paths, &scope)
}

fn rebuild_from_paths_with_scope(
    database_paths: &[PathBuf],
    rollout_paths: &[PathBuf],
    scope: &provider_sync::SessionScope,
) -> anyhow::Result<Vec<SessionSummary>> {
    let mut sessions = HashMap::<String, SessionSummary>::new();
    for mut session in provider_sync::list_database_sessions_from_paths(database_paths, scope)? {
        session.id = truncate_text(&session.id, MAX_SESSION_ID_CHARS);
        session.title = truncate_text(&session.title, 512);
        session.provider = provider_sync::normalize_provider(&session.provider);
        session.original_provider = provider_sync::normalize_provider(&session.original_provider);
        session.cwd = truncate_text(&session.cwd, MAX_SESSION_PATH_CHARS);
        sessions.insert(session.id.clone(), session);
    }
    for path in rollout_paths {
        let Ok(file) = fs::File::open(path) else {
            continue;
        };
        let mut metadata = None;
        let mut title = String::new();
        let mut has_user_event = false;
        for line in std::io::BufReader::new(file)
            .take(MAX_ROLLOUT_INDEX_BYTES)
            .lines()
        {
            let Ok(line) = line else { break };
            if line.len() > MAX_ROLLOUT_LINE_BYTES {
                continue;
            }
            let Ok(record) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if record.get("type").and_then(Value::as_str) == Some("session_meta") {
                metadata = record.get("payload").cloned();
            }
            if matches!(
                record.pointer("/payload/type").and_then(Value::as_str),
                Some("user_message" | "user_input")
            ) {
                has_user_event = true;
                if title.is_empty() {
                    title = record
                        .pointer("/payload/message")
                        .or_else(|| record.pointer("/payload/text"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .lines()
                        .find(|line| !line.trim().is_empty())
                        .unwrap_or_default()
                        .chars()
                        .take(160)
                        .collect();
                }
            }
            if metadata.is_some() && has_user_event && !title.is_empty() {
                break;
            }
        }
        let Some(metadata) = metadata else { continue };
        let Some(raw_id) = metadata.get("id").and_then(Value::as_str) else {
            continue;
        };
        let id = truncate_text(raw_id, MAX_SESSION_ID_CHARS);
        let Some(archived) = scope.root_archived(&id) else {
            continue;
        };
        let provider = provider_sync::normalize_provider(
            metadata
                .get("model_provider")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        let updated_at = fs::metadata(path)
            .ok()
            .and_then(|value| value.modified().ok())
            .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|value| value.as_secs() as i64)
            .unwrap_or_default();
        let rollout = path.display().to_string();
        sessions
            .entry(id.clone())
            .and_modify(|session| {
                session.provider = provider.clone();
                session.original_provider = provider.clone();
                session.source_rollout = Some(rollout.clone());
                session.has_user_event |= has_user_event;
                session.updated_at = session.updated_at.max(updated_at);
                if session.title.is_empty() {
                    session.title = title.clone();
                }
            })
            .or_insert(SessionSummary {
                identity: format!("rollout:{rollout}"),
                id,
                title,
                provider: provider.clone(),
                cwd: truncate_text(
                    &metadata
                        .get("cwd")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .trim_start_matches(r"\\?\")
                        .replace('\\', "/"),
                    MAX_SESSION_PATH_CHARS,
                ),
                archived,
                updated_at,
                source_db: String::new(),
                source_rollout: Some(rollout),
                original_provider: provider,
                has_user_event,
            });
    }
    let mut output = sessions.into_values().collect::<Vec<_>>();
    output.sort_by_key(|session| std::cmp::Reverse(session.updated_at));
    Ok(output)
}

pub fn session_page(
    index: &SessionIndex,
    home: &Path,
    query: &str,
    page: usize,
    page_size: usize,
    archived: bool,
) -> Result<PageResult<SessionSummary>, AppError> {
    let sessions = index.load_recent(home)?;
    let query = query.trim();
    let normalized_query = if query.is_ascii() {
        Cow::Borrowed(query)
    } else {
        Cow::Owned(query.to_lowercase())
    };
    let query = normalized_query.as_ref();
    let matches = |session: &SessionSummary| {
        session.archived == archived && (query.is_empty() || session_matches_query(session, query))
    };
    let total = sessions.iter().filter(|session| matches(session)).count();
    let last_page = total.max(1).div_ceil(page_size);
    let page = page.clamp(1, last_page);
    let start = (page - 1).saturating_mul(page_size);
    let items = sessions
        .iter()
        .filter(|session| matches(session))
        .skip(start)
        .take(page_size)
        .cloned()
        .collect();
    Ok(PageResult {
        items,
        total,
        page,
        page_size,
    })
}

fn session_matches_query(session: &SessionSummary, normalized_query: &str) -> bool {
    if normalized_query.is_empty() {
        return true;
    }
    let query_is_ascii = normalized_query.is_ascii();
    [
        session.id.as_str(),
        session.title.as_str(),
        session.provider.as_str(),
        session.cwd.as_str(),
    ]
    .into_iter()
    .any(|value| {
        if query_is_ascii {
            value
                .as_bytes()
                .windows(normalized_query.len())
                .any(|window| window.eq_ignore_ascii_case(normalized_query.as_bytes()))
        } else {
            !value.is_ascii() && value.to_lowercase().contains(normalized_query)
        }
    })
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    // 单趟定位截断边界，避免先 `chars().count()` 再 `chars().take()`
    // 造成的双遍历（会话索引重建时对每个字段都会调用）。
    let cut = value
        .char_indices()
        .enumerate()
        .find_map(|(count, (index, _))| (count == max_chars).then_some(index));
    cut.map_or_else(|| value.to_owned(), |index| value[..index].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn reuses_unchanged_index_and_rebuilds_after_source_change() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let rollout = sessions.join("rollout.jsonl");
        fs::write(
            &rollout,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"one\",\"model_provider\":\"custom\",\"cwd\":\"C:/work\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"first\"}}\n"
            ),
        )
        .unwrap();
        let index = SessionIndex::default();

        let first = index.load(temp.path()).unwrap();
        let unchanged = index.load(temp.path()).unwrap();
        assert!(Arc::ptr_eq(&first, &unchanged));
        assert_eq!(first[0].title, "first");

        fs::write(
            &rollout,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"one\",\"model_provider\":\"custom\",\"cwd\":\"C:/work\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"updated title\"}}\n"
            ),
        )
        .unwrap();
        let updated = index.load(temp.path()).unwrap();
        assert!(!Arc::ptr_eq(&first, &updated));
        assert_eq!(updated[0].title, "updated title");
    }

    #[test]
    fn recent_load_skips_rewalking_sources_until_forced_refresh() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let rollout = sessions.join("rollout.jsonl");
        fs::write(
            &rollout,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"one\",\"model_provider\":\"custom\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"first\"}}\n"
            ),
        )
        .unwrap();
        let index = SessionIndex::default();
        let first = index.load(temp.path()).unwrap();

        fs::write(
            &rollout,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"one\",\"model_provider\":\"custom\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"changed\"}}\n"
            ),
        )
        .unwrap();
        let recent = index.load_recent(temp.path()).unwrap();
        assert!(Arc::ptr_eq(&first, &recent));

        let refreshed = index.load(temp.path()).unwrap();
        assert!(!Arc::ptr_eq(&first, &refreshed));
        assert_eq!(refreshed[0].title, "changed");
    }

    #[test]
    fn sqlite_wal_changes_invalidate_the_index() {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("state_5.sqlite");
        let connection = Connection::open(&database).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    model_provider TEXT,
                    cwd TEXT,
                    archived INTEGER,
                    updated_at INTEGER
                );
                INSERT INTO threads VALUES('one','First','custom','C:/one',0,1);",
            )
            .unwrap();
        let index = SessionIndex::default();
        let first = index.load(temp.path()).unwrap();
        assert_eq!(first.len(), 1);

        connection
            .execute(
                "INSERT INTO threads VALUES('two','Second','custom','C:/two',0,2)",
                [],
            )
            .unwrap();
        let updated = index.load(temp.path()).unwrap();
        assert_eq!(updated.len(), 2);
        assert!(!Arc::ptr_eq(&first, &updated));
    }

    #[test]
    fn refresh_paths_updates_only_affected_rollout() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let a = sessions.join("a.jsonl");
        let b = sessions.join("b.jsonl");
        let write_rollout = |path: &Path, id: &str, provider: &str| {
            fs::write(
                path,
                format!(
                    "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"model_provider\":\"{provider}\"}}}}\n\
                     {{\"type\":\"event_msg\",\"payload\":{{\"type\":\"user_message\",\"message\":\"{id} title\"}}}}\n"
                ),
            )
            .unwrap();
        };
        write_rollout(&a, "one", "openai");
        write_rollout(&b, "two", "openai");
        let index = SessionIndex::default();
        let first = index.load(temp.path()).unwrap();
        assert_eq!(
            first.iter().find(|s| s.id == "one").unwrap().provider,
            "openai"
        );
        assert_eq!(
            first.iter().find(|s| s.id == "two").unwrap().provider,
            "openai"
        );

        write_rollout(&a, "one", "custom");
        index
            .refresh_paths(temp.path(), std::slice::from_ref(&a))
            .unwrap();

        let refreshed = index.load(temp.path()).unwrap();
        assert_eq!(
            refreshed.iter().find(|s| s.id == "one").unwrap().provider,
            "custom"
        );
        assert_eq!(
            refreshed.iter().find(|s| s.id == "two").unwrap().provider,
            "openai"
        );
    }

    #[test]
    fn refresh_paths_invalidates_when_unaffected_source_changed() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let a = sessions.join("a.jsonl");
        let b = sessions.join("b.jsonl");
        let write_rollout = |path: &Path, id: &str, provider: &str| {
            fs::write(
                path,
                format!(
                    "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"model_provider\":\"{provider}\"}}}}\n\
                     {{\"type\":\"event_msg\",\"payload\":{{\"type\":\"user_message\",\"message\":\"{id} title\"}}}}\n"
                ),
            )
            .unwrap();
        };
        write_rollout(&a, "one", "openai");
        write_rollout(&b, "two", "openai");
        let index = SessionIndex::default();
        index.load(temp.path()).unwrap();

        write_rollout(&a, "one", "custom");
        write_rollout(&b, "two", "custom-longer");
        index
            .refresh_paths(temp.path(), std::slice::from_ref(&a))
            .unwrap();

        let refreshed = index.load(temp.path()).unwrap();
        assert_eq!(
            refreshed.iter().find(|s| s.id == "one").unwrap().provider,
            "custom"
        );
        assert_eq!(
            refreshed.iter().find(|s| s.id == "two").unwrap().provider,
            "custom"
        );
    }

    #[test]
    fn refresh_paths_keeps_rollout_provider_when_only_database_changed() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let rollout = sessions.join("one.jsonl");
        fs::write(
            &rollout,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"one\",\"model_provider\":\"custom\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"one title\"}}\n"
            ),
        )
        .unwrap();
        let database = temp.path().join("state_5.sqlite");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(
                    id TEXT PRIMARY KEY, model_provider TEXT, title TEXT, cwd TEXT,
                    archived INTEGER, updated_at INTEGER
                );
                INSERT INTO threads VALUES('one','openai','db title','C:/one',0,1);",
            )
            .unwrap();
        drop(connection);

        let index = SessionIndex::default();
        let first = index.load(temp.path()).unwrap();
        assert_eq!(
            first.iter().find(|s| s.id == "one").unwrap().provider,
            "custom"
        );

        let connection = Connection::open(&database).unwrap();
        connection
            .execute(
                "UPDATE threads SET model_provider='custom' WHERE id='one'",
                [],
            )
            .unwrap();
        drop(connection);
        index
            .refresh_paths(temp.path(), std::slice::from_ref(&database))
            .unwrap();

        let refreshed = index.load(temp.path()).unwrap();
        let session = refreshed.iter().find(|s| s.id == "one").unwrap();
        assert_eq!(session.provider, "custom");
        assert_eq!(
            session.source_rollout.as_deref(),
            Some(rollout.to_str().unwrap())
        );
    }

    #[test]
    fn refresh_paths_applies_repair_affected_paths() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let sessions = home.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let rollout = sessions.join("one.jsonl");
        fs::write(
            &rollout,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"one\",\"model_provider\":\"openai\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"one title\"}}\n"
            ),
        )
        .unwrap();

        let index = SessionIndex::default();
        let first = index.load(&home).unwrap();
        assert_eq!(first[0].provider, "openai");

        let (_, paths) = provider_sync::repair_after_connection_switch_preserving_history_with_guard_at_with_paths(
            &home,
            "custom",
            None,
            || Ok(true),
        )
        .unwrap();
        assert_eq!(paths, vec![rollout.clone()]);

        index.refresh_paths(&home, &paths).unwrap();
        let refreshed = index.load(&home).unwrap();
        assert_eq!(refreshed[0].provider, "custom");
    }

    #[test]
    fn oversized_rollout_lines_do_not_block_later_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let rollout = sessions.join("rollout.jsonl");
        let mut content = "x".repeat(MAX_ROLLOUT_LINE_BYTES + 1);
        content.push('\n');
        content.push_str(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"safe\",\"model_provider\":\"openai\"}}\n",
        );
        content.push_str(
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"kept\"}}\n",
        );
        fs::write(&rollout, content).unwrap();

        let index = SessionIndex::default();
        let indexed = index.load(temp.path()).unwrap();

        assert_eq!(indexed.len(), 1);
        assert_eq!(indexed[0].id, "safe");
        assert_eq!(indexed[0].title, "kept");
    }

    #[test]
    fn session_pages_filter_without_changing_totals_or_page_boundaries() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        for (id, provider, cwd, title) in [
            ("one", "custom", "C:/alpha", "Alpha task"),
            ("two", "openai", "C:/beta", "中文任务"),
            ("three", "custom", "C:/gamma", "Gamma task"),
        ] {
            let contents = format!(
                "{}\n{}\n",
                serde_json::json!({
                    "type": "session_meta",
                    "payload": {"id": id, "model_provider": provider, "cwd": cwd}
                }),
                serde_json::json!({
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": title}
                })
            );
            fs::write(sessions.join(format!("{id}.jsonl")), contents).unwrap();
        }
        let index = SessionIndex::default();

        let first = session_page(&index, temp.path(), "", 1, 2, false).unwrap();
        let second = session_page(&index, temp.path(), "", 2, 2, false).unwrap();
        assert_eq!(first.total, 3);
        assert_eq!(first.items.len(), 2);
        assert_eq!(second.total, 3);
        assert_eq!(second.items.len(), 1);
        let mut ids = first
            .items
            .into_iter()
            .chain(second.items)
            .map(|session| session.id)
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, ["one", "three", "two"]);

        let ascii = session_page(&index, temp.path(), "ALPHA", 1, 10, false).unwrap();
        assert_eq!(ascii.total, 1);
        assert_eq!(ascii.items[0].id, "one");
        let unicode = session_page(&index, temp.path(), "中文", 1, 10, false).unwrap();
        assert_eq!(unicode.total, 1);
        assert_eq!(unicode.items[0].id, "two");

        let overflow = session_page(&index, temp.path(), "ALPHA", 8, 2, false).unwrap();
        assert_eq!(overflow.page, 1);
        assert_eq!(overflow.total, 1);
        assert_eq!(overflow.items[0].id, "one");
    }

    fn summary(id: &str, title: &str, provider: &str, cwd: &str) -> SessionSummary {
        SessionSummary {
            identity: id.into(),
            id: id.into(),
            title: title.into(),
            provider: provider.into(),
            cwd: cwd.into(),
            archived: false,
            updated_at: 0,
            source_db: "rollout.sqlite".into(),
            source_rollout: None,
            original_provider: provider.into(),
            has_user_event: true,
        }
    }

    #[test]
    fn session_query_matches_ascii_fields_case_insensitively() {
        let session = summary(
            "session-01",
            "修复登录 Bug",
            "openai-official",
            "/Users/iqboost/project",
        );
        assert!(session_matches_query(&session, "OPENAI"));
        assert!(session_matches_query(&session, "bug"));
        assert!(session_matches_query(&session, "project"));
        assert!(session_matches_query(&session, ""));
        assert!(!session_matches_query(&session, "absent"));
    }

    #[test]
    fn session_query_never_matches_ascii_field_with_cjk_query() {
        let session = summary(
            "session-01",
            "fix login bug",
            "openai-official",
            "/Users/iqboost/project",
        );
        assert!(!session_matches_query(&session, "登录"));
    }

    #[test]
    fn session_query_matches_cjk_title_with_normalized_lowercase() {
        let session = summary(
            "session-01",
            "修复 登录 Bug 与 Setup",
            "openai-official",
            "/Users/iqboost/project",
        );
        assert!(session_matches_query(&session, "登录"));
        assert!(session_matches_query(&session, "setup"));
        assert!(!session_matches_query(&session, "配额"));
    }
}
