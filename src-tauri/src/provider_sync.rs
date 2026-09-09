use crate::{
    models::{AppError, DatabaseScan, RepairResult, RepairScan, RepairTarget, SessionSummary},
    platform,
    storage::{atomic_write, atomic_write_if_unchanged},
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, backup::Backup};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fs,
    io::{BufRead, Read},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};
use walkdir::WalkDir;

const MAX_ROLLOUT_SCAN_BYTES: u64 = 2 * 1024 * 1024;
const MAX_REPAIR_ROLLOUT_BYTES: u64 = 256 * 1024 * 1024;
// Each planner can hold both the original and repaired rollout. Keep the cap
// stricter than available_parallelism so preflight memory remains predictable.
const MAX_REPAIR_WORKERS: usize = 4;
const MAX_REPAIR_WARNINGS: usize = 100;
const MAX_WARNING_CHARS: usize = 1_000;
// v3 invalidates entries produced by the former rewrite-and-clear-projection
// policy.  A cached rollout must have been checked by the byte-preserving
// policy before it can be skipped again.
const MANIFEST_VERSION: u32 = 3;
const MANIFEST_PREFIX_BYTES: usize = 4 * 1024;
const THREAD_HISTORY_FILE: &str = "thread_history_1.sqlite";

fn repair_worker_count(file_count: usize, available_parallelism: usize) -> usize {
    file_count
        .min(available_parallelism.max(1))
        .min(MAX_REPAIR_WORKERS)
}

/// 修复引擎唯一接受的路由目标。两个变体都表示清除会话模型覆盖，让 Codex
/// 继承刚刚激活的连接的当前默认模型；不把任何历史 model 名称带到新上游。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionRoutingTarget {
    OpenAi,
    Custom,
}

impl SessionRoutingTarget {
    fn parse(value: &str) -> Result<Self, AppError> {
        match value.trim() {
            "openai" => Ok(Self::OpenAi),
            "custom" => Ok(Self::Custom),
            _ => Err(AppError::InvalidConfig(
                "只能在 OpenAI 账号与第三方 API 之间更新会话归属。".into(),
            )),
        }
    }

    fn provider(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Custom => "custom",
        }
    }
}

/// 可展示和可修复的本地会话范围。根会话只用于列表；其子代理后代仅用于修复。
#[derive(Default)]
pub struct SessionScope {
    roots: HashMap<String, bool>,
    eligible: HashSet<String>,
    eligible_rollouts: HashSet<PathBuf>,
}

impl SessionScope {
    pub fn root_archived(&self, id: &str) -> Option<bool> {
        self.roots.get(id).copied()
    }

    #[cfg(test)]
    pub fn contains(&self, id: &str) -> bool {
        self.eligible.contains(id)
    }

    fn rollout_is_eligible(&self, path: &Path) -> bool {
        self.eligible_rollouts.contains(path)
    }

    fn eligible_ids(&self) -> Vec<&str> {
        let mut ids = self.eligible.iter().map(String::as_str).collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }
}

#[derive(Default)]
struct ScopeFacts {
    has_catalog: bool,
    roots: HashMap<String, bool>,
    parents: HashMap<String, String>,
    rollout_ids: Vec<(PathBuf, String)>,
}

/// 统一识别会话根及其子代理后代。新版 catalog 是活跃根的唯一权威来源；旧库
/// 没有 catalog 时回退到非归档、非网页端且没有父会话的 threads 记录。
pub fn session_scope(
    database_paths: &[PathBuf],
    rollout_paths: &[PathBuf],
) -> anyhow::Result<SessionScope> {
    let mut facts = ScopeFacts::default();
    for path in database_paths {
        collect_database_scope(path, &mut facts)?;
    }
    for path in rollout_paths {
        collect_rollout_scope(path, &mut facts)?;
    }

    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    for (child, parent) in &facts.parents {
        if child != parent {
            children
                .entry(parent.clone())
                .or_default()
                .push(child.clone());
        }
    }
    let mut eligible = HashSet::new();
    let mut queue = facts.roots.keys().cloned().collect::<VecDeque<_>>();
    while let Some(id) = queue.pop_front() {
        if !eligible.insert(id.clone()) {
            continue;
        }
        if let Some(next) = children.get(&id) {
            queue.extend(next.iter().cloned());
        }
    }
    let eligible_rollouts = facts
        .rollout_ids
        .into_iter()
        .filter_map(|(path, id)| eligible.contains(&id).then_some(path))
        .collect();
    Ok(SessionScope {
        roots: facts.roots,
        eligible,
        eligible_rollouts,
    })
}

fn collect_database_scope(path: &Path, facts: &mut ScopeFacts) -> anyhow::Result<()> {
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let catalog = table_columns(&db, "local_thread_catalog")?;
    let hosts = table_columns(&db, "local_thread_catalog_hosts")?;
    let has_catalog = catalog.contains("thread_id");
    facts.has_catalog |= has_catalog;
    if has_catalog
        && let Some((catalog_key, host_key)) = catalog_host_join(&catalog, &hosts)
        && catalog.contains("source_kind")
        && catalog.contains("missing_candidate")
        && hosts.contains("host_kind")
    {
        let source_kind = "source_kind";
        let missing = "missing_candidate";
        let host_kind = "host_kind";
        let sql = format!(
            "SELECT c.thread_id FROM local_thread_catalog c JOIN local_thread_catalog_hosts h ON c.{catalog_key}=h.{host_key} WHERE h.{host_kind}='local' AND COALESCE(c.{source_kind},'')<>'chatgpt' AND COALESCE(c.{missing},0)=0"
        );
        let mut statement = db.prepare(&sql)?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .flatten();
        for id in ids {
            facts.roots.entry(id).or_insert(false);
        }
    }

    let thread_columns = table_columns(&db, "threads")?;
    if !thread_columns.contains("id") {
        return Ok(());
    }
    let archived = choose(&thread_columns, &["archived"], "0");
    let source = choose(&thread_columns, &["source"], "NULL");
    let sql = format!("SELECT id, COALESCE({archived},0), {source} FROM threads");
    let mut statement = db.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1).unwrap_or_default() != 0,
            row.get::<_, Option<String>>(2).unwrap_or_default(),
        ))
    })?;
    for row in rows.flatten() {
        let (id, archived, source) = row;
        let (parent, chatgpt) = source.as_deref().map(scope_source).unwrap_or((None, false));
        if let Some(parent) = parent {
            facts.parents.insert(id.clone(), parent);
        }
        if chatgpt {
            facts.roots.remove(&id);
            continue;
        }
        if archived && !facts.parents.contains_key(&id) {
            facts.roots.insert(id.clone(), true);
        } else if !has_catalog && !facts.parents.contains_key(&id) {
            facts.roots.entry(id).or_insert(false);
        }
    }
    Ok(())
}

fn catalog_host_join(
    catalog: &HashSet<String>,
    hosts: &HashSet<String>,
) -> Option<(&'static str, &'static str)> {
    [
        ("host_id", "id"),
        ("host_id", "host_id"),
        ("thread_id", "thread_id"),
    ]
    .into_iter()
    .find(|(left, right)| catalog.contains(*left) && hosts.contains(*right))
}

fn collect_rollout_scope(path: &Path, facts: &mut ScopeFacts) -> anyhow::Result<()> {
    let file = fs::File::open(path)?;
    for line in std::io::BufReader::new(file)
        .take(MAX_ROLLOUT_SCAN_BYTES)
        .lines()
    {
        let line = line?;
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if record.get("type").and_then(Value::as_str) != Some("session_meta") {
            continue;
        }
        let payload = record.get("payload").unwrap_or(&Value::Null);
        let Some(id) = payload.get("id").and_then(Value::as_str) else {
            break;
        };
        let (parent, chatgpt) = payload
            .get("source")
            .map(scope_source_value)
            .unwrap_or((None, false));
        if let Some(parent) = parent {
            facts.parents.insert(id.to_owned(), parent);
        }
        if !chatgpt
            && !facts.parents.contains_key(id)
            && (path
                .components()
                .any(|part| part.as_os_str() == "archived_sessions")
                || !facts.has_catalog)
        {
            facts.roots.insert(
                id.to_owned(),
                path.components()
                    .any(|part| part.as_os_str() == "archived_sessions"),
            );
        }
        facts.rollout_ids.push((path.to_path_buf(), id.to_owned()));
        break;
    }
    Ok(())
}

fn scope_source(source: &str) -> (Option<String>, bool) {
    serde_json::from_str::<Value>(source)
        .ok()
        .map(|value| scope_source_value(&value))
        .unwrap_or((None, source.eq_ignore_ascii_case("chatgpt")))
}

fn scope_source_value(value: &Value) -> (Option<String>, bool) {
    let parent = value
        .pointer("/subagent/thread_spawn/parent_thread_id")
        .or_else(|| value.pointer("/subagent/parent_thread_id"))
        .or_else(|| value.get("parent_thread_id"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let chatgpt = value == "chatgpt"
        || value
            .get("source_kind")
            .or_else(|| value.get("kind"))
            .or_else(|| value.get("type"))
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("chatgpt"));
    (parent, chatgpt)
}

pub fn scan(codex_home: &Path) -> RepairScan {
    let mut warnings = vec![];
    let mut omitted_warnings = 0;
    let all_rollouts = rollout_files(codex_home);
    let databases = database_paths(codex_home);
    let scope = match session_scope(&databases, &all_rollouts) {
        Ok(scope) => scope,
        Err(error) => {
            return RepairScan {
                current_provider: configured_provider(codex_home),
                targets: vec![],
                rollout_files: 0,
                session_meta_count: 0,
                databases: vec![],
                warnings: vec![format!("无法确定本地会话范围：{error}")],
            };
        }
    };
    let rollouts = all_rollouts
        .into_iter()
        .filter(|path| scope.rollout_is_eligible(path))
        .collect::<Vec<_>>();
    let mut providers = BTreeMap::<String, BTreeSet<String>>::new();
    let mut counts = BTreeMap::<String, usize>::new();
    let mut session_meta_count = 0;
    for path in &rollouts {
        match rollout_provider(path) {
            Ok(Some(provider)) => {
                session_meta_count += 1;
                *counts.entry(provider.clone()).or_default() += 1;
                providers
                    .entry(provider)
                    .or_default()
                    .insert("rollout".into());
            }
            Ok(None) => {}
            Err(error) => push_warning(
                &mut warnings,
                &mut omitted_warnings,
                format!("无法读取 {}：{error}", path.display()),
            ),
        }
    }
    let mut database_scans = vec![];
    for path in databases {
        match inspect_database(&path, &scope) {
            Ok(Some(inspection)) => {
                for (provider, provider_count) in &inspection.providers {
                    *counts.entry(provider.clone()).or_default() += *provider_count as usize;
                    providers
                        .entry(provider.clone())
                        .or_default()
                        .insert("sqlite".into());
                }
                database_scans.push(DatabaseScan {
                    path: path.display().to_string(),
                    schema: inspection.schema,
                    thread_count: inspection.thread_count,
                });
            }
            Ok(None) => {}
            Err(error) => push_warning(
                &mut warnings,
                &mut omitted_warnings,
                format!("无法检查 {}：{error}", path.display()),
            ),
        }
    }
    finish_warnings(&mut warnings, omitted_warnings);
    let current_provider = configured_provider(codex_home);
    providers
        .entry(current_provider.clone())
        .or_default()
        .insert("config".into());
    RepairScan {
        current_provider: current_provider.clone(),
        targets: providers
            .into_iter()
            .map(|(id, sources)| {
                let count = counts.get(&id).copied().unwrap_or(0);
                RepairTarget {
                    current: id == current_provider,
                    id,
                    sources: sources.into_iter().collect(),
                    count,
                }
            })
            .collect(),
        rollout_files: rollouts.len(),
        session_meta_count,
        databases: database_scans,
        warnings,
    }
}

pub fn configured_provider(codex_home: &Path) -> String {
    fs::read_to_string(codex_home.join("config.toml"))
        .ok()
        .and_then(|text| text.parse::<toml_edit::DocumentMut>().ok())
        .and_then(|doc| {
            doc.get("model_provider")
                .and_then(toml_edit::Item::as_str)
                .map(normalize_provider)
        })
        .unwrap_or_else(|| "openai".into())
}

pub fn normalize_provider(value: &str) -> String {
    if value.trim().is_empty() || value.eq_ignore_ascii_case("openai") {
        "openai".into()
    } else {
        "custom".into()
    }
}

#[cfg(test)]
pub fn repair(codex_home: &Path, target: &str) -> Result<RepairResult, AppError> {
    repair_with_history_mode_and_guard_with_paths(codex_home, target, false, None, || Ok(true))
        .map(|(result, _)| result)
}

#[cfg(test)]
pub fn repair_after_connection_switch(
    codex_home: &Path,
    target: &str,
) -> Result<RepairResult, AppError> {
    repair_with_history_mode_and_guard_with_paths(codex_home, target, true, None, || Ok(true))
        .map(|(result, _)| result)
}

#[cfg(test)]
pub(crate) fn repair_with_guard(
    codex_home: &Path,
    target: &str,
    may_write: impl FnMut() -> Result<bool, AppError>,
) -> Result<RepairResult, AppError> {
    Ok(repair_with_guard_with_paths(codex_home, target, may_write)?.0)
}

/// 与 [`repair_with_guard`] 相同，但额外返回本次实际修改过的会话文件路径，
/// 供上层只刷新受影响来源的会话索引，避免全量重建。
#[cfg(test)]
pub(crate) fn repair_with_guard_with_paths(
    codex_home: &Path,
    target: &str,
    may_write: impl FnMut() -> Result<bool, AppError>,
) -> Result<(RepairResult, Vec<PathBuf>), AppError> {
    repair_with_guard_with_paths_for_app(codex_home, target, None, may_write)
}

/// 与 [`repair_with_guard_with_paths`] 相同，但为历史迁移器指定 Codex Desktop
/// 的配置路径，避免自定义安装回退到 PATH 中的其他 CLI。
pub(crate) fn repair_with_guard_with_paths_for_app(
    codex_home: &Path,
    target: &str,
    configured_app: Option<&str>,
    may_write: impl FnMut() -> Result<bool, AppError>,
) -> Result<(RepairResult, Vec<PathBuf>), AppError> {
    repair_with_history_mode_and_guard_with_paths(
        codex_home,
        target,
        false,
        configured_app,
        may_write,
    )
}

/// 切换账号/服务后只更新会话的 provider 元数据，不重序列化 JSONL 历史。
///
/// Codex 新版用 rollout 的字节偏移和 ordinal 关联 `thread_history_1.sqlite`。
/// `repair_rollout` 适合用户主动执行的完整修复，但重序列化会改变偏移；
/// 激活切换路径必须使用这个原位版本。`openai` 和 `custom` 长度相同，因此
/// 常见的切换可以在不改变文件布局的情况下修复旧会话的路由。
///
/// 修复采用持久化清单做增量：清单命中（路径+长度+修改时间+前缀指纹一致且已
/// 指向目标 provider）的 rollout 不再读取正文；未命中或内容变化的 rollout 才
/// 流式解析。整次切换先只读预检，任一 rollout 或数据库无法确认可修复时在写入
/// 前整体终止；写入阶段任何一步失败都会用备份恢复所有已修改文件并返回错误，
/// 让上层回滚连接切换，绝不返回部分成功。
#[cfg(test)]
pub(crate) fn repair_after_connection_switch_preserving_history_with_guard(
    codex_home: &Path,
    target: &str,
    may_write: impl FnMut() -> Result<bool, AppError>,
) -> Result<RepairResult, AppError> {
    repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
        codex_home, target, None, None, may_write,
    )
    .map(|(result, _)| result)
}

#[cfg(test)]
pub(crate) fn repair_after_connection_switch_preserving_history_with_guard_at(
    codex_home: &Path,
    target: &str,
    manifest_path: Option<&Path>,
    may_write: impl FnMut() -> Result<bool, AppError>,
) -> Result<RepairResult, AppError> {
    repair_after_connection_switch_preserving_history_with_guard_at_with_paths(
        codex_home,
        target,
        manifest_path,
        may_write,
    )
    .map(|(result, _)| result)
}

/// 与 [`repair_after_connection_switch_preserving_history_with_guard_at`] 相同，
/// 但额外返回本次实际修改过的 rollout 与数据库路径，供上层只刷新受影响来源。
#[cfg(test)]
pub(crate) fn repair_after_connection_switch_preserving_history_with_guard_at_with_paths(
    codex_home: &Path,
    target: &str,
    manifest_path: Option<&Path>,
    may_write: impl FnMut() -> Result<bool, AppError>,
) -> Result<(RepairResult, Vec<PathBuf>), AppError> {
    repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
        codex_home,
        target,
        manifest_path,
        None,
        may_write,
    )
}

/// 与 [`repair_after_connection_switch_preserving_history_with_guard_at_with_paths`] 相同，
/// 但历史迁移器将使用传入的已配置 Codex Desktop 路径定位内置 CLI。
pub(crate) fn repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
    codex_home: &Path,
    target: &str,
    manifest_path: Option<&Path>,
    configured_app: Option<&str>,
    may_write: impl FnMut() -> Result<bool, AppError>,
) -> Result<(RepairResult, Vec<PathBuf>), AppError> {
    repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app_impl(
        codex_home,
        target,
        manifest_path,
        configured_app,
        may_write,
        #[cfg(test)]
        None,
        #[cfg(test)]
        None,
    )
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
enum RepairTestStage {
    AfterBackup(PathBuf),
    AfterRolloutWrite(PathBuf),
    BeforeDatabaseCommit(PathBuf),
    AfterHistoryModeCommitted,
    AfterMigration,
}

#[cfg(test)]
type RepairTestHook<'a> = dyn FnMut(RepairTestStage) -> anyhow::Result<()> + 'a;

#[cfg(test)]
type MigrationTestRunner = for<'path, 'app, 'plans> fn(
    &'path Path,
    Option<&'app str>,
    &'plans [LogicalThreadHistoryRecoveryPlan],
) -> anyhow::Result<MigrationReport>;

#[cfg(test)]
fn repair_after_connection_switch_preserving_history_with_test_hooks(
    codex_home: &Path,
    target: &str,
    manifest_path: Option<&Path>,
    configured_app: Option<&str>,
    may_write: impl FnMut() -> Result<bool, AppError>,
    test_hook: &mut RepairTestHook<'_>,
    migration_runner: Option<MigrationTestRunner>,
) -> Result<(RepairResult, Vec<PathBuf>), AppError> {
    repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app_impl(
        codex_home,
        target,
        manifest_path,
        configured_app,
        may_write,
        Some(test_hook),
        migration_runner,
    )
}

fn repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app_impl(
    codex_home: &Path,
    target: &str,
    manifest_path: Option<&Path>,
    configured_app: Option<&str>,
    mut may_write: impl FnMut() -> Result<bool, AppError>,
    #[cfg(test)] mut test_hook: Option<&mut RepairTestHook<'_>>,
    #[cfg(test)] migration_runner: Option<MigrationTestRunner>,
) -> Result<(RepairResult, Vec<PathBuf>), AppError> {
    let started = Instant::now();
    let target = SessionRoutingTarget::parse(target)?;
    let target_provider = target.provider();
    let manifest = load_manifest(manifest_path);
    let all_rollouts = rollout_files(codex_home);
    let all_databases = database_paths(codex_home);
    let databases = repair_database_paths(codex_home, &all_databases)
        .map_err(|error| AppError::Internal(format!("会话数据库：{error}")))?;
    let scope = session_scope(&all_databases, &all_rollouts)
        .map_err(|error| AppError::Internal(format!("无法确定本地会话范围：{error}")))?;
    let rollouts = all_rollouts
        .into_iter()
        .filter(|path| scope.rollout_is_eligible(path))
        .collect::<Vec<_>>();
    let mut affected_paths: Vec<PathBuf> = Vec::new();
    let mut result = RepairResult {
        target_provider: target_provider.to_owned(),
        files_scanned: rollouts.len(),
        databases_scanned: databases.len(),
        ..RepairResult::default()
    };

    // ---- 只读预检：任一 rollout / 数据库无法确认可修复即整体终止 ----
    // 有界并行读取+解析各 rollout 文件，避免大量会话耗尽线程或内存。
    let mut manifest_entries: HashMap<&str, Vec<&SessionRepairManifestEntry>> = HashMap::new();
    for entry in &manifest.entries {
        manifest_entries
            .entry(entry.path.as_str())
            .or_default()
            .push(entry);
    }
    let next_rollout = AtomicUsize::new(0);
    let available_parallelism = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let worker_count = repair_worker_count(rollouts.len(), available_parallelism);
    let planned: Vec<(PathBuf, PlannedRollout)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..worker_count)
            .map(|_| {
                let manifest_entries = &manifest_entries;
                let rollouts = &rollouts;
                let next_rollout = &next_rollout;
                scope.spawn(move || {
                    let mut results = Vec::new();
                    loop {
                        let index = next_rollout.fetch_add(1, Ordering::Relaxed);
                        let Some(path) = rollouts.get(index) else {
                            break;
                        };
                        let path_key = path.to_string_lossy();
                        let entries = manifest_entries
                            .get(path_key.as_ref())
                            .map(Vec::as_slice)
                            .unwrap_or_default();
                        let result = plan_rollout(path, target_provider, entries)
                            .map(|plan| (path.clone(), plan))
                            .map_err(|error| {
                                AppError::Internal(format!("会话文件 {}：{error}", path.display()))
                            });
                        results.push((index, result));
                    }
                    results
                })
            })
            .collect();
        let mut indexed = Vec::with_capacity(rollouts.len());
        for handle in handles {
            match handle.join() {
                Ok(results) => indexed.extend(results),
                Err(error) => {
                    return Err(AppError::Internal(format!("会话修复线程异常：{error:?}")));
                }
            }
        }
        indexed.sort_by_key(|(index, _)| *index);
        indexed
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Result<Vec<_>, _>>()
    })?;
    let mut repair_plans: Vec<RolloutPlan> = Vec::new();
    let mut cached_entries: Vec<SessionRepairManifestEntry> = Vec::new();
    let mut recorded: Vec<RecordedRollout> = Vec::new();
    for (path, PlannedRollout { plan, cached_entry }) in planned {
        match plan {
            RolloutPlan::Cached => {
                result.files_cached += 1;
                if let Some(entry) = cached_entry {
                    cached_entries.push(entry);
                }
            }
            RolloutPlan::Matching { session_meta_count } => {
                result.files_opened += 1;
                result.files_skipped += 1;
                recorded.push(RecordedRollout {
                    path,
                    session_meta_count,
                });
            }
            plan @ RolloutPlan::Repair {
                session_meta_count, ..
            } => {
                result.files_opened += 1;
                recorded.push(RecordedRollout {
                    path,
                    session_meta_count,
                });
                repair_plans.push(plan);
            }
        }
    }

    let mut database_plans: Vec<(PathBuf, usize, String)> = Vec::new();
    for path in &databases {
        match preflight_database(path, target_provider, &scope) {
            Ok(rows) if rows > 0 => database_plans.push((path.clone(), rows, file_sha256(path)?)),
            Ok(_) => {}
            Err(error) => {
                return Err(AppError::Internal(format!(
                    "会话数据库 {}：{error}",
                    path.display()
                )));
            }
        }
    }
    // provider 标识长度变化会移动后续 JSONL 字节。分页历史库保存的是这些字节偏移，
    // 因此即使现有投影在写入前有效，也必须在同一事务中要求 Codex 重建它。
    let length_changing_rollouts = repair_plans
        .iter()
        .filter_map(|plan| match plan {
            RolloutPlan::Repair {
                path,
                write_mode: RolloutWriteMode::RewriteAndReproject,
                ..
            } => Some(path.clone()),
            _ => None,
        })
        .collect::<HashSet<_>>();
    let recovery_plans = preflight_paginated_history_recovery(
        codex_home,
        &scope,
        &rollouts,
        &length_changing_rollouts,
    )
    .map_err(|error| AppError::Internal(format!("会话历史恢复：{error}")))?;
    if !recovery_plans.is_empty() {
        ensure_migration_uses_backed_up_databases(codex_home)
            .map_err(|error| AppError::Internal(format!("会话历史恢复：{error}")))?;
    }
    let history_database = codex_home.join(THREAD_HISTORY_FILE);
    let history_database_hash = history_database
        .is_file()
        .then(|| file_sha256(&history_database))
        .transpose()?;
    // ---- 执行阶段：备份 + 写入，任一步失败整体回滚 ----
    if !repair_plans.is_empty() || !database_plans.is_empty() || !recovery_plans.is_empty() {
        if !may_write()? {
            return Err(AppError::Internal(
                "Codex 已重新运行或修复目标已变化，切换已终止。".into(),
            ));
        }
        let mut backup = RepairBackup::create()
            .map_err(|error| AppError::Internal(format!("无法创建回滚备份：{error}")))?;
        let expected_rollouts = repair_plans
            .iter()
            .filter_map(|plan| match plan {
                RolloutPlan::Repair {
                    path,
                    repaired_sha256,
                    ..
                } => Some((path.clone(), repaired_sha256.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        let stable_expected_rollouts = expected_rollouts
            .iter()
            .filter(|(path, _)| {
                !recovery_plans
                    .iter()
                    .any(|plan| plan.rollouts.iter().any(|rollout| rollout.path == *path))
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut backed_paths = HashSet::new();
        let mut write_started = false;
        let mut written_paths = HashSet::new();
        let commit = (|| -> anyhow::Result<()> {
            // 所有目标在首次写入前完成哈希守卫和备份；后续阶段不得再把“刚写过的
            // 文件”当作备份源。
            for plan in &repair_plans {
                let RolloutPlan::Repair {
                    path,
                    original_sha256,
                    ..
                } = plan
                else {
                    continue;
                };
                let current = fs::read(path)?;
                if file_sha256_bytes(&current) != *original_sha256 {
                    anyhow::bail!("Codex 正在更新会话 {}，切换已终止并回滚。", path.display());
                }
                backup.add_bytes(path, &current)?;
                backed_paths.insert(path.clone());
                #[cfg(test)]
                run_repair_test_hook(&mut test_hook, RepairTestStage::AfterBackup(path.clone()))?;
            }
            for (path, _, expected_hash) in &database_plans {
                if &file_sha256(path)? != expected_hash {
                    anyhow::bail!(
                        "Codex 正在更新会话数据库 {}，切换已终止并回滚。",
                        path.display()
                    );
                }
                backup.add_sqlite_database(path)?;
                backed_paths.insert(path.clone());
                #[cfg(test)]
                run_repair_test_hook(&mut test_hook, RepairTestStage::AfterBackup(path.clone()))?;
            }
            if !recovery_plans.is_empty() {
                if let Some(expected_hash) = history_database_hash.as_ref()
                    && file_sha256(&history_database)? != *expected_hash
                {
                    anyhow::bail!("Codex 正在更新历史投影，切换已终止并回滚。");
                }
                if backed_paths.insert(history_database.clone()) {
                    backup.add_sqlite_database(&history_database)?;
                    #[cfg(test)]
                    run_repair_test_hook(
                        &mut test_hook,
                        RepairTestStage::AfterBackup(history_database.clone()),
                    )?;
                }
                let state_database = codex_home.join("state_5.sqlite");
                if backed_paths.insert(state_database.clone()) {
                    backup.add_sqlite_database(&state_database)?;
                    #[cfg(test)]
                    run_repair_test_hook(
                        &mut test_hook,
                        RepairTestStage::AfterBackup(state_database.clone()),
                    )?;
                }
                for logical_plan in &recovery_plans {
                    for plan in &logical_plan.rollouts {
                        let current = fs::read(&plan.path)?;
                        if file_sha256_bytes(&current) != plan.original_sha256 {
                            anyhow::bail!(
                                "Codex 正在更新会话 {}，历史恢复已终止并回滚。",
                                plan.path.display()
                            );
                        }
                        if backed_paths.insert(plan.path.clone()) {
                            backup.add_bytes(&plan.path, &current)?;
                            #[cfg(test)]
                            run_repair_test_hook(
                                &mut test_hook,
                                RepairTestStage::AfterBackup(plan.path.clone()),
                            )?;
                        }
                    }
                }
            }
            for plan in repair_plans.drain(..) {
                let RolloutPlan::Repair {
                    path,
                    original_sha256,
                    repaired_sha256,
                    meta_count,
                    session_meta_count,
                    ..
                } = plan
                else {
                    unreachable!("只有需要写入的 rollout 才会进入执行阶段");
                };
                if !may_write()? {
                    anyhow::bail!("Codex 已重新运行或修复目标已变化，切换已终止并回滚。");
                }
                let analysis = analyze_rollout_metadata_in_place(&path, target_provider)?;
                let Some(write) = analysis.write else {
                    anyhow::bail!("Codex 正在更新会话 {}，切换已终止并回滚。", path.display());
                };
                if write.original_sha256 != original_sha256
                    || file_sha256_bytes(&write.repaired) != repaired_sha256
                    || write.meta_count != meta_count
                    || analysis.session_meta_count != session_meta_count
                {
                    anyhow::bail!("Codex 正在更新会话 {}，切换已终止并回滚。", path.display());
                }
                let changed = match atomic_write_if_unchanged_and_track(
                    &path,
                    &write.original,
                    &write.repaired,
                    &mut written_paths,
                ) {
                    Ok(changed) => changed,
                    Err(error) => {
                        write_started |= written_paths.contains(&path);
                        return Err(error);
                    }
                };
                if !changed {
                    anyhow::bail!("Codex 正在更新会话 {}，切换已终止并回滚。", path.display());
                }
                write_started = true;
                #[cfg(test)]
                run_repair_test_hook(
                    &mut test_hook,
                    RepairTestStage::AfterRolloutWrite(path.clone()),
                )?;
                result.files_modified += 1;
                result.session_meta_updated += meta_count;
                affected_paths.push(path);
            }
            for (path, expected_rows, expected_hash) in &database_plans {
                if !may_write()? {
                    anyhow::bail!("Codex 已重新运行或修复目标已变化，切换已终止并回滚。");
                }
                if &file_sha256(path)? != expected_hash {
                    anyhow::bail!(
                        "Codex 正在更新会话数据库 {}，切换已终止并回滚。",
                        path.display()
                    );
                }
                #[cfg(test)]
                run_repair_test_hook(
                    &mut test_hook,
                    RepairTestStage::BeforeDatabaseCommit(path.clone()),
                )?;
                let rows = repair_database_commit(
                    path,
                    target_provider,
                    &scope,
                    *expected_rows,
                    &mut may_write,
                )?;
                write_started = true;
                written_paths.insert(path.clone());
                result.rows_updated += rows;
                result.databases_updated += 1;
                affected_paths.push(path.clone());
            }
            if !recovery_plans.is_empty() {
                let state_database = codex_home.join("state_5.sqlite");
                for logical_plan in &recovery_plans {
                    for plan in &logical_plan.rollouts {
                        if !may_write()? {
                            anyhow::bail!(
                                "Codex 已重新运行或修复目标已变化，历史恢复已终止并回滚。"
                            );
                        }
                        let current = fs::read(&plan.path)?;
                        let current_sha256 = file_sha256_bytes(&current);
                        if current_sha256 != plan.original_sha256
                            && !expected_rollouts.iter().any(|(path, expected)| {
                                path == &plan.path && expected == &current_sha256
                            })
                        {
                            anyhow::bail!(
                                "Codex 正在更新会话 {}，历史恢复已终止并回滚。",
                                plan.path.display()
                            );
                        }
                        let legacy =
                            mark_rollout_history_legacy(&current, &plan.logical_thread_id)?;
                        let changed = match atomic_write_if_unchanged_and_track(
                            &plan.path,
                            &current,
                            &legacy,
                            &mut written_paths,
                        ) {
                            Ok(changed) => changed,
                            Err(error) => {
                                write_started |= written_paths.contains(&plan.path);
                                return Err(error);
                            }
                        };
                        if !changed {
                            anyhow::bail!(
                                "Codex 正在更新会话 {}，历史恢复已终止并回滚。",
                                plan.path.display()
                            );
                        }
                        write_started = true;
                        affected_paths.push(plan.path.clone());
                    }
                }
                mark_database_history_legacy(&state_database, &recovery_plans, &mut may_write)?;
                write_started = true;
                written_paths.insert(state_database.clone());
                written_paths.insert(history_database.clone());
                #[cfg(test)]
                run_repair_test_hook(&mut test_hook, RepairTestStage::AfterHistoryModeCommitted)?;
                #[cfg(test)]
                let migration_report = if let Some(runner) = migration_runner {
                    runner(codex_home, configured_app, &recovery_plans)?
                } else {
                    run_codex_history_migration(codex_home, configured_app, &recovery_plans)?
                };
                #[cfg(not(test))]
                let migration_report =
                    run_codex_history_migration(codex_home, configured_app, &recovery_plans)?;
                #[cfg(test)]
                run_repair_test_hook(&mut test_hook, RepairTestStage::AfterMigration)?;
                verify_paginated_history_recovery(
                    &state_database,
                    &history_database,
                    &recovery_plans,
                    &migration_report,
                    target_provider,
                )?;
                affected_paths.push(state_database);
                affected_paths.push(history_database.clone());
            }
            verify_repair_commit(
                &stable_expected_rollouts,
                &database_plans,
                target_provider,
                &scope,
            )?;
            Ok(())
        })();
        if let Err(error) = commit {
            if !write_started {
                let _ = backup.cleanup();
                return Err(AppError::Internal(format!(
                    "会话归属修复未完成，写入前预检或备份失败：{error}"
                )));
            }
            let restore_error = backup.restore_selected(&written_paths);
            let residue_error = cleanup_known_migration_residue(codex_home, &recovery_plans);
            return match restore_error {
                Ok(()) => {
                    if let Err(residue) = residue_error {
                        return Err(AppError::Internal(format!(
                            "会话归属修复未完成且迁移副产物清理失败：{error}（清理错误：{residue}；备份保留在 {}）",
                            backup.dir.display()
                        )));
                    }
                    backup.cleanup().map_err(|cleanup| {
                        AppError::Internal(format!(
                            "会话归属修复未完成，已回滚全部修改，但无法清理备份 {}：{cleanup}",
                            backup.dir.display()
                        ))
                    })?;
                    Err(AppError::Internal(format!(
                        "会话归属修复未完成，已回滚全部修改：{error}"
                    )))
                }
                Err(restore) => {
                    let residue = residue_error
                        .err()
                        .map(|residue| format!("；迁移副产物清理错误：{residue}"))
                        .unwrap_or_default();
                    Err(AppError::Internal(format!(
                        "会话归属修复未完成且回滚失败：{error}（恢复错误：{restore}{residue}；备份保留在 {}）",
                        backup.dir.display()
                    )))
                }
            };
        }
        let _ = backup.cleanup();
    }

    // ---- 成功后按修复结果重建清单，淘汰过期条目 ----
    let mut next_manifest = SessionRepairManifest {
        version: MANIFEST_VERSION,
        ..SessionRepairManifest::default()
    };
    next_manifest.entries.extend(cached_entries);
    next_manifest.entries.extend(
        recorded
            .iter()
            .map(|record| record.to_entry(target_provider)),
    );
    save_manifest(manifest_path, &next_manifest);

    result.verification_passed = result.files_failed == 0 && result.warnings.is_empty();
    result.repair_complete = result.verification_passed;
    result.elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    Ok((result, affected_paths))
}

#[cfg(test)]
fn run_repair_test_hook(
    hook: &mut Option<&mut RepairTestHook<'_>>,
    stage: RepairTestStage,
) -> anyhow::Result<()> {
    if let Some(hook) = hook.as_deref_mut() {
        hook(stage)?;
    }
    Ok(())
}

fn atomic_write_if_unchanged_and_track(
    path: &Path,
    expected: &[u8],
    replacement: &[u8],
    written_paths: &mut HashSet<PathBuf>,
) -> anyhow::Result<bool> {
    track_atomic_write_result(path, replacement, written_paths, || {
        atomic_write_if_unchanged(path, expected, replacement)
    })
}

fn track_atomic_write_result(
    path: &Path,
    replacement: &[u8],
    written_paths: &mut HashSet<PathBuf>,
    write: impl FnOnce() -> anyhow::Result<bool>,
) -> anyhow::Result<bool> {
    match write() {
        Ok(true) => {
            written_paths.insert(path.to_path_buf());
            Ok(true)
        }
        Ok(false) => Ok(false),
        Err(error) => {
            // `replace_temporary` may report a post-rename durability error.  Only
            // register a target when it now contains this engine's exact bytes;
            // an Ok(false) CAS conflict and unrelated external bytes remain untouched.
            if fs::read(path).is_ok_and(|current| current == replacement) {
                written_paths.insert(path.to_path_buf());
            }
            Err(error)
        }
    }
}

fn cleanup_known_migration_residue(
    codex_home: &Path,
    plans: &[LogicalThreadHistoryRecoveryPlan],
) -> anyhow::Result<()> {
    if plans.is_empty() {
        return Ok(());
    }
    let journals = codex_home.join("rollout-migrations");
    for plan in plans {
        let pending = journals.join(format!("{}.pending", plan.logical_thread_id));
        remove_known_migration_file(&pending)?;
        for rollout in &plan.rollouts {
            let name = rollout
                .path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("rollout 路径缺少文件名"))?
                .to_string_lossy();
            let temporary = rollout
                .path
                .with_file_name(format!(".{name}.paginated.tmp"));
            remove_known_migration_file(&temporary)?;
        }
    }
    if journals.is_dir() && fs::read_dir(&journals)?.next().is_none() {
        fs::remove_dir(&journals)?;
    }
    Ok(())
}

fn remove_known_migration_file(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if !path.is_file() {
        anyhow::bail!("已知迁移副产物路径不是普通文件：{}", path.display());
    }
    fs::remove_file(path)?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SessionRepairManifestEntry {
    path: String,
    file_length: u64,
    file_modified_at_ms: Option<i64>,
    prefix_sha256: Option<String>,
    provider: String,
    target: String,
    session_meta_count: usize,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SessionRepairManifest {
    version: u32,
    entries: Vec<SessionRepairManifestEntry>,
}

fn default_manifest_path() -> PathBuf {
    crate::storage::data_root().join("session_repair_manifest.json")
}

fn load_manifest(manifest_path: Option<&Path>) -> SessionRepairManifest {
    match manifest_path {
        Some(path) => load_manifest_at(path),
        None => load_manifest_at(&default_manifest_path()),
    }
}

fn load_manifest_at(path: &Path) -> SessionRepairManifest {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .filter(|manifest: &SessionRepairManifest| manifest.version == MANIFEST_VERSION)
        .unwrap_or_default()
}

fn save_manifest(manifest_path: Option<&Path>, manifest: &SessionRepairManifest) {
    match manifest_path {
        Some(path) => save_manifest_at(path, manifest),
        None => save_manifest_at(&default_manifest_path(), manifest),
    }
}

fn save_manifest_at(path: &Path, manifest: &SessionRepairManifest) {
    let Ok(json) = serde_json::to_vec_pretty(manifest) else {
        return;
    };
    // 清单只是增量缓存；写失败不影响本次修复结果，下次切换会重建。
    let _ = atomic_write(path, &json);
}

fn rollout_prefix_sha256(path: &Path) -> Option<String> {
    let mut file = fs::File::open(path).ok()?;
    let mut buffer = vec![0u8; MANIFEST_PREFIX_BYTES];
    let mut filled = 0;
    while filled < MANIFEST_PREFIX_BYTES {
        let count = file.read(&mut buffer[filled..]).ok()?;
        if count == 0 {
            break;
        }
        filled += count;
    }
    let mut hasher = Sha256::new();
    hasher.update(&buffer[..filled]);
    Some(format!("{:x}", hasher.finalize()))
}

fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn file_sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone, Debug)]
struct PhysicalRolloutRecoveryPlan {
    logical_thread_id: String,
    rollout_id: String,
    path: PathBuf,
    original_sha256: String,
    semantic_sha256: String,
    recovery_reason: Option<HistoryRecoveryReason>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum HistoryRecoveryReason {
    ProjectionInvalid,
    RewriteAndReproject { projection_was_healthy: bool },
}

#[derive(Clone, Debug)]
struct LogicalThreadHistoryRecoveryPlan {
    logical_thread_id: String,
    rollouts: Vec<PhysicalRolloutRecoveryPlan>,
}

#[derive(Clone, Debug)]
struct HistoryPosition {
    rollout_id: String,
    end_ordinal_exclusive: u64,
    end_byte_offset: u64,
}

#[derive(Clone, Debug)]
struct RolloutRecoveryInfo {
    logical_thread_id: String,
    rollout_id: String,
    history_mode: Option<String>,
    first_projectable_end_offset: Option<u64>,
    is_subagent: bool,
    history_base: Option<HistoryPosition>,
    first_ordinal: u64,
    #[cfg(test)]
    last_ordinal: u64,
    next_ordinal: u64,
    first_ordinal_gap: Option<(u64, u64, u64)>,
    record_start_offsets: BTreeMap<u64, u64>,
    record_end_offsets: BTreeMap<u64, u64>,
}

#[derive(Clone, Debug)]
struct RolloutMetadataSummary {
    logical_thread_id: String,
    history_mode: Option<String>,
    is_subagent: bool,
}

#[derive(Default)]
struct ForcedPaginationEvidence {
    history_mode: Option<String>,
    has_non_null_history_base: bool,
}

#[derive(Clone, Debug)]
struct ResolvedLineageRollout {
    plan: PhysicalRolloutRecoveryPlan,
    history_base: Option<HistoryPosition>,
}

fn preflight_paginated_history_recovery(
    codex_home: &Path,
    scope: &SessionScope,
    rollouts: &[PathBuf],
    force_recovery_rollouts: &HashSet<PathBuf>,
) -> anyhow::Result<Vec<LogicalThreadHistoryRecoveryPlan>> {
    // 等长 provider 原位替换不会移动任何 rollout 字节或 ordinal，也不需要
    // 修复既有 projection。分页恢复仅服务于确实改变记录宽度的 rollout；
    // 否则旧数据中可继续消费的 projection 前缀或既有瑕疵会错误阻断连接切换。
    if force_recovery_rollouts.is_empty() {
        return Ok(Vec::new());
    }
    // 变长 provider 写入会移动后续记录的字节偏移。写入前必须综合 session_meta
    // history_mode/history_base、state_5.sqlite 与已有 projection 库判定是否存在分页
    // 证据；不能把缺省 history_mode 一律当作 legacy。
    let mut forced_evidence = HashMap::new();
    for path in force_recovery_rollouts {
        let original = fs::read(path)?;
        forced_evidence.insert(path.clone(), forced_pagination_evidence(&original)?);
    }
    if !force_recovery_rollouts.is_empty() {
        ensure_migration_uses_backed_up_databases(codex_home)?;
    }
    let history_path = codex_home.join(THREAD_HISTORY_FILE);
    let mut history = if !force_recovery_rollouts.is_empty() && history_path.is_file() {
        Some(Connection::open_with_flags(
            &history_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?)
    } else {
        None
    };
    let history_has_projection_evidence = history
        .as_ref()
        .map(history_connection_has_projection_evidence)
        .transpose()?
        .unwrap_or(false);
    let forced_has_pagination_evidence = !force_recovery_rollouts.is_empty()
        && (history_has_projection_evidence
            || forced_evidence.values().any(|evidence| {
                evidence.history_mode.as_deref() == Some("paginated")
                    || evidence.has_non_null_history_base
            }));
    let state_path = codex_home.join("state_5.sqlite");
    if !state_path.is_file() {
        if forced_has_pagination_evidence {
            anyhow::bail!(
                "变长 rollout 存在分页证据，需要 state_5.sqlite 才能安全重建历史：{}",
                force_recovery_rollouts
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join("；")
            );
        }
        return Ok(Vec::new());
    }
    let state = Connection::open_with_flags(
        &state_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let columns = table_columns(&state, "threads")?;
    if !columns.contains("id") || !columns.contains("history_mode") {
        if forced_has_pagination_evidence {
            anyhow::bail!(
                "变长 rollout 存在分页证据，需要 state_5.sqlite.threads 的 id/history_mode 列才可安全重建"
            );
        }
        return Ok(Vec::new());
    }
    let forced_logical_thread_ids = force_recovery_rollouts
        .iter()
        .map(|path| {
            rollout_metadata_summary(&fs::read(path)?).map(|summary| summary.logical_thread_id)
        })
        .collect::<anyhow::Result<HashSet<_>>>()?;

    if history.is_none() && history_path.is_file() {
        history = Some(Connection::open_with_flags(
            &history_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?);
    }
    if let Some(history) = history.as_ref() {
        for (table, required) in [
            (
                "thread_history_projection_state",
                &[
                    "thread_id",
                    "next_rollout_byte_offset",
                    "next_rollout_ordinal",
                ][..],
            ),
            (
                "thread_items",
                &["thread_id", "rollout_ordinal", "item_json"][..],
            ),
            (
                "thread_turns",
                &[
                    "thread_id",
                    "rollout_ordinal",
                    "rollout_byte_offset",
                    "rollout_end_ordinal",
                    "rollout_end_byte_offset",
                ][..],
            ),
        ] {
            let columns = table_columns(history, table)?;
            for column in required {
                if !columns.contains(*column) {
                    anyhow::bail!("历史投影表 {table} 缺少必要列 {column}");
                }
            }
        }
    }

    let mut grouped =
        BTreeMap::<String, Vec<(PhysicalRolloutRecoveryPlan, RolloutRecoveryInfo)>>::new();
    let mut state_modes = HashMap::<String, Option<String>>::new();
    for path in rollouts {
        if !scope.rollout_is_eligible(path) {
            continue;
        }
        let original = fs::read(path)?;
        let summary = rollout_metadata_summary(&original)?;
        if !scope.eligible.contains(&summary.logical_thread_id) {
            continue;
        }
        if !forced_logical_thread_ids.contains(&summary.logical_thread_id) {
            continue;
        }
        let state_mode = match state_modes.get(&summary.logical_thread_id) {
            Some(mode) => mode.clone(),
            None => {
                let mode = state
                    .query_row(
                        "SELECT history_mode FROM threads WHERE id=?1",
                        [&summary.logical_thread_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                state_modes.insert(summary.logical_thread_id.clone(), mode.clone());
                mode
            }
        };
        let forced = forced_evidence.get(path);
        let forced_has_history_base =
            forced.is_some_and(|evidence| evidence.has_non_null_history_base);
        let forced_declares_paginated =
            forced.is_some_and(|evidence| evidence.history_mode.as_deref() == Some("paginated"));
        let state_is_paginated = state_mode.as_deref() == Some("paginated");
        if force_recovery_rollouts.contains(path)
            && summary.history_mode.as_deref() == Some("legacy")
            && (state_is_paginated || forced_has_history_base)
        {
            anyhow::bail!(
                "rollout {} 的 legacy history_mode 与分页证据冲突，拒绝变长改写",
                path.display()
            );
        }
        if !state_is_paginated {
            if force_recovery_rollouts.contains(path)
                && (forced_declares_paginated
                    || forced_has_history_base
                    || history_has_projection_evidence)
            {
                anyhow::bail!(
                    "变长 rollout {} 与 state_5.sqlite/history 投影的分页状态不一致，拒绝变长改写",
                    path.display()
                );
            }
            continue;
        }
        if summary
            .history_mode
            .as_deref()
            .is_some_and(|mode| mode != "paginated")
        {
            continue;
        }
        let info = rollout_recovery_info(path, &original)
            .map_err(|error| anyhow::anyhow!("rollout {}：{error}", path.display()))?;
        if info.is_subagent != summary.is_subagent
            || info.logical_thread_id != summary.logical_thread_id
        {
            anyhow::bail!("rollout 元数据预检前后不一致，拒绝恢复");
        }
        let projection_was_healthy = paginated_projection_is_valid(history.as_ref(), &info)?;
        // 旧版跳号历史只能沿用已验证的投影。当前官方迁移器不能保证重放缺号
        // 记录时保持原边界；变长改写和损坏投影都必须在任何写入前拒绝。
        if (force_recovery_rollouts.contains(path) || !projection_was_healthy)
            && let Some((previous, ordinal, offset)) = info.first_ordinal_gap
        {
            anyhow::bail!(
                "rollout {} 在字节 {offset} 的 ordinal {previous}→{ordinal} 存在缺号；{}，无法安全重投影，已在写入前拒绝恢复",
                path.display(),
                if force_recovery_rollouts.contains(path) {
                    "provider 变长改写需要重投影"
                } else {
                    "现有 projection 或记录边界不健康"
                }
            );
        }
        let recovery_reason = if force_recovery_rollouts.contains(path) {
            Some(HistoryRecoveryReason::RewriteAndReproject {
                projection_was_healthy,
            })
        } else if !projection_was_healthy {
            Some(HistoryRecoveryReason::ProjectionInvalid)
        } else {
            None
        };
        grouped
            .entry(info.logical_thread_id.clone())
            .or_default()
            .push((
                PhysicalRolloutRecoveryPlan {
                    logical_thread_id: info.logical_thread_id.clone(),
                    rollout_id: info.rollout_id.clone(),
                    path: path.clone(),
                    original_sha256: file_sha256_bytes(&original),
                    semantic_sha256: rollout_semantic_sha256(&original)?,
                    recovery_reason,
                },
                info,
            ));
    }
    let mut plans = Vec::new();
    let all_entries = grouped
        .values()
        .flat_map(|entries| entries.iter().cloned())
        .collect::<Vec<_>>();
    for (logical_thread_id, entries) in grouped {
        if !entries
            .iter()
            .any(|(plan, _)| plan.recovery_reason.is_some())
        {
            continue;
        }
        let rollouts =
            resolve_complete_paginated_lineage(&logical_thread_id, entries, &all_entries)?;
        if rollouts.len() != 1
            || rollouts[0].history_base.is_some()
            || rollouts[0].plan.rollout_id != logical_thread_id
        {
            let paths = rollouts
                .iter()
                .map(|plan| plan.plan.path.display().to_string())
                .collect::<Vec<_>>()
                .join("；");
            anyhow::bail!(
                "会话 {logical_thread_id} 的分页历史不兼容当前 Codex 按 logical thread 的迁移器；已在写入前拒绝恢复（{} 个 rollout）：{paths}",
                rollouts.len()
            );
        }
        plans.push(LogicalThreadHistoryRecoveryPlan {
            logical_thread_id,
            rollouts: rollouts.into_iter().map(|entry| entry.plan).collect(),
        });
    }
    Ok(plans)
}

fn rollout_metadata_summary(original: &[u8]) -> anyhow::Result<RolloutMetadataSummary> {
    for segment in original.split_inclusive(|byte| *byte == b'\n') {
        let line = segment.strip_suffix(b"\n").unwrap_or(segment);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if record.get("type").and_then(Value::as_str) != Some("session_meta") {
            continue;
        }
        let logical_thread_id = record
            .pointer("/payload/id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("session_meta 缺少 logical thread ID"))
            .and_then(|value| {
                uuid::Uuid::parse_str(value)
                    .map(|value| value.to_string())
                    .map_err(|_| anyhow::anyhow!("session_meta logical thread ID 不合法"))
            })?;
        return Ok(RolloutMetadataSummary {
            logical_thread_id,
            history_mode: record
                .pointer("/payload/history_mode")
                .and_then(Value::as_str)
                .map(str::to_owned),
            is_subagent: session_meta_parent_thread_id(&record).is_some(),
        });
    }
    anyhow::bail!("未找到可验证的 session_meta.id")
}

fn forced_pagination_evidence(original: &[u8]) -> anyhow::Result<ForcedPaginationEvidence> {
    for segment in original.split_inclusive(|byte| *byte == b'\n') {
        let line = segment.strip_suffix(b"\n").unwrap_or(segment);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if record.get("type").and_then(Value::as_str) == Some("session_meta") {
            return Ok(ForcedPaginationEvidence {
                history_mode: record
                    .pointer("/payload/history_mode")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                has_non_null_history_base: record
                    .pointer("/payload/history_base")
                    .is_some_and(|value| !value.is_null()),
            });
        }
    }
    Ok(ForcedPaginationEvidence::default())
}

fn history_connection_has_projection_evidence(history: &Connection) -> anyhow::Result<bool> {
    let columns = table_columns(history, "thread_history_projection_state")?;
    if !columns.contains("thread_id")
        || !columns.contains("next_rollout_byte_offset")
        || !columns.contains("next_rollout_ordinal")
    {
        return Ok(false);
    }
    let count: i64 = history.query_row(
        "SELECT COUNT(*) FROM thread_history_projection_state",
        [],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn rollout_recovery_info(path: &Path, original: &[u8]) -> anyhow::Result<RolloutRecoveryInfo> {
    let (path_logical_thread_id, rollout_id) = rollout_ids_from_path(path)?;
    let mut metadata_thread_id = None;
    let mut history_mode = None;
    let mut first_projectable_end_offset = None;
    let mut is_subagent = false;
    let mut history_base = None;
    let mut saw_primary_session_meta = false;
    let mut first_ordinal = None;
    let mut previous_ordinal: Option<u64> = None;
    let mut next_ordinal = 0;
    let mut first_ordinal_gap = None;
    let mut record_start_offsets = BTreeMap::new();
    let mut record_end_offsets = BTreeMap::new();
    let mut byte_offset = 0_u64;
    for segment in original.split_inclusive(|byte| *byte == b'\n') {
        let record_start = byte_offset;
        byte_offset = byte_offset
            .checked_add(u64::try_from(segment.len())?)
            .ok_or_else(|| anyhow::anyhow!("rollout 字节长度溢出"))?;
        let line = segment.strip_suffix(b"\n").unwrap_or(segment);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            anyhow::bail!("rollout 包含空记录，无法验证 lineage");
        }
        let record: Value = serde_json::from_slice(line)?;
        let ordinal = record
            .get("ordinal")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("rollout 缺少可验证的 ordinal"))?;
        if let Some(previous) = previous_ordinal {
            if ordinal <= previous {
                anyhow::bail!(
                    "rollout 字节 {record_start} 的 ordinal {previous}→{ordinal} 重复或倒退，无法验证 lineage"
                );
            }
            if ordinal != next_ordinal {
                first_ordinal_gap.get_or_insert((previous, ordinal, record_start));
            }
        }
        // 与官方 read_projection_steps 的 next_line_ordinal 一致：cursor 是
        // 已处理记录的 exclusive ordinal，不是行数；同时要求 SQLite 可表示。
        next_ordinal = ordinal
            .checked_add(1)
            .filter(|next| i64::try_from(*next).is_ok())
            .ok_or_else(|| {
                anyhow::anyhow!("rollout 字节 {record_start} 的 ordinal 超出 SQLite 整数范围")
            })?;
        first_ordinal.get_or_insert(ordinal);
        previous_ordinal = Some(ordinal);
        record_start_offsets.insert(ordinal, record_start);
        record_end_offsets.insert(ordinal, byte_offset);
        match record.get("type").and_then(Value::as_str) {
            Some("session_meta") => {
                // 官方 fork/subagent rollout 可能把父会话的 session_meta 作为第
                // 二条及后续历史记录带入。只有首条 session_meta 描述当前物理文件；
                // 后续记录仍参与 ordinal/字节边界，但不能覆盖当前文件的身份和 lineage。
                if saw_primary_session_meta {
                    continue;
                }
                saw_primary_session_meta = true;
                metadata_thread_id = Some(
                    record
                        .pointer("/payload/id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow::anyhow!("首条 session_meta 缺少 logical thread ID"))
                        .and_then(|value| {
                            uuid::Uuid::parse_str(value)
                                .map(|value| value.to_string())
                                .map_err(|_| {
                                    anyhow::anyhow!("首条 session_meta logical thread ID 不合法")
                                })
                        })?,
                );
                history_mode = record
                    .pointer("/payload/history_mode")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                is_subagent = session_meta_parent_thread_id(&record).is_some();
                history_base = record
                    .pointer("/payload/history_base")
                    .filter(|value| !value.is_null())
                    .map(parse_history_position)
                    .transpose()?;
            }
            Some("event_msg")
                if record.pointer("/payload/type").and_then(Value::as_str)
                    == Some("task_started") =>
            {
                first_projectable_end_offset.get_or_insert(byte_offset);
            }
            Some("event_msg")
                if record.pointer("/payload/type").and_then(Value::as_str)
                    == Some("user_message") =>
            {
                first_projectable_end_offset.get_or_insert(byte_offset);
            }
            Some("response_item")
                if record.pointer("/payload/type").and_then(Value::as_str) == Some("message")
                    && record.pointer("/payload/role").and_then(Value::as_str) == Some("user") =>
            {
                first_projectable_end_offset.get_or_insert(byte_offset);
            }
            _ => {}
        }
    }
    let logical_thread_id =
        metadata_thread_id.ok_or_else(|| anyhow::anyhow!("未找到可验证的 session_meta.id"))?;
    if logical_thread_id != path_logical_thread_id {
        anyhow::bail!("rollout 文件名 logical ID 与 session_meta.id 不一致，拒绝猜测物理 ID");
    }
    Ok(RolloutRecoveryInfo {
        logical_thread_id,
        rollout_id,
        history_mode,
        first_projectable_end_offset,
        is_subagent,
        history_base,
        first_ordinal: first_ordinal
            .ok_or_else(|| anyhow::anyhow!("rollout 不包含可验证的记录"))?,
        #[cfg(test)]
        last_ordinal: previous_ordinal
            .ok_or_else(|| anyhow::anyhow!("rollout 不包含可验证的记录"))?,
        next_ordinal,
        first_ordinal_gap,
        record_start_offsets,
        record_end_offsets,
    })
}

fn rollout_ids_from_path(path: &Path) -> anyhow::Result<(String, String)> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("rollout 文件名不是有效 UTF-8"))?;
    let stem = name
        .strip_prefix("rollout-")
        .and_then(|value| value.strip_suffix(".jsonl"))
        .ok_or_else(|| anyhow::anyhow!("rollout 文件名不符合官方格式"))?;
    let timestamp = stem
        .get(..19)
        .ok_or_else(|| anyhow::anyhow!("rollout 文件名缺少时间戳"))?;
    chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H-%M-%S")
        .map_err(|_| anyhow::anyhow!("rollout 文件名时间戳不合法"))?;
    let ids = stem
        .get(19..)
        .and_then(|value| value.strip_prefix('-'))
        .ok_or_else(|| anyhow::anyhow!("rollout 文件名缺少 UUID"))?;
    let (logical, physical) = match ids.split_once('_') {
        Some((logical, physical)) if !physical.contains('_') => (logical, physical),
        Some(_) => anyhow::bail!("rollout 文件名包含歧义的物理 UUID"),
        None => (ids, ids),
    };
    let logical = uuid::Uuid::parse_str(logical)
        .map_err(|_| anyhow::anyhow!("rollout 文件名 logical UUID 不合法"))?
        .to_string();
    let physical = uuid::Uuid::parse_str(physical)
        .map_err(|_| anyhow::anyhow!("rollout 文件名 physical UUID 不合法"))?
        .to_string();
    Ok((logical, physical))
}

fn parse_history_position(value: &Value) -> anyhow::Result<HistoryPosition> {
    Ok(HistoryPosition {
        rollout_id: value
            .get("thread_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("history_base 缺少物理 rollout ID"))
            .and_then(|value| {
                uuid::Uuid::parse_str(value)
                    .map(|value| value.to_string())
                    .map_err(|_| anyhow::anyhow!("history_base 物理 rollout ID 不合法"))
            })?,
        end_ordinal_exclusive: value
            .get("end_ordinal_exclusive")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("history_base 缺少结束 ordinal"))?,
        end_byte_offset: value
            .get("end_byte_offset")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("history_base 缺少结束字节偏移"))?,
    })
}

fn resolve_complete_paginated_lineage(
    logical_thread_id: &str,
    entries: Vec<(PhysicalRolloutRecoveryPlan, RolloutRecoveryInfo)>,
    all_entries: &[(PhysicalRolloutRecoveryPlan, RolloutRecoveryInfo)],
) -> anyhow::Result<Vec<ResolvedLineageRollout>> {
    let mut by_rollout = BTreeMap::new();
    let mut children = BTreeMap::<String, String>::new();
    for (plan, info) in entries {
        if by_rollout
            .insert(info.rollout_id.clone(), (plan, info))
            .is_some()
        {
            anyhow::bail!("会话 {logical_thread_id} 存在重复物理 rollout ID");
        }
    }
    for (_, external_info) in all_entries {
        let Some(base) = external_info.history_base.as_ref() else {
            continue;
        };
        if by_rollout.contains_key(&base.rollout_id)
            && external_info.logical_thread_id != logical_thread_id
        {
            anyhow::bail!(
                "会话 {logical_thread_id} 的物理 rollout 被其他 logical thread 的 history_base 引用，拒绝迁移"
            );
        }
    }
    for (rollout_id, (_, info)) in &by_rollout {
        let Some(base) = info.history_base.as_ref() else {
            continue;
        };
        let Some((_, parent)) = by_rollout.get(&base.rollout_id) else {
            anyhow::bail!(
                "会话 {logical_thread_id} 的 rollout {rollout_id} 缺少 history_base 父文件"
            );
        };
        if parent.history_mode.as_deref() != Some("paginated")
            || parent.record_end_offsets.get(
                &base
                    .end_ordinal_exclusive
                    .checked_sub(1)
                    .ok_or_else(|| anyhow::anyhow!("history_base 结束 ordinal 非法"))?,
            ) != Some(&base.end_byte_offset)
            || info.first_ordinal != base.end_ordinal_exclusive
        {
            anyhow::bail!(
                "会话 {logical_thread_id} 的 rollout {rollout_id} history_base 不指向真实 paginated 记录边界"
            );
        }
        if children
            .insert(base.rollout_id.clone(), rollout_id.clone())
            .is_some()
        {
            anyhow::bail!("会话 {logical_thread_id} 存在分叉 rollout lineage，拒绝猜测恢复顺序");
        }
    }
    let roots = by_rollout
        .iter()
        .filter_map(|(rollout_id, (_, info))| {
            info.history_base.is_none().then_some(rollout_id.to_owned())
        })
        .collect::<Vec<_>>();
    if roots.len() != 1 {
        anyhow::bail!("会话 {logical_thread_id} 的 rollout lineage 缺少唯一根");
    }
    let mut resolved = Vec::with_capacity(by_rollout.len());
    let mut current = roots[0].clone();
    loop {
        let (plan, _) = by_rollout
            .get(&current)
            .ok_or_else(|| anyhow::anyhow!("rollout lineage 内部状态不一致"))?;
        let (_, info) = by_rollout
            .get(&current)
            .ok_or_else(|| anyhow::anyhow!("rollout lineage 内部状态不一致"))?;
        resolved.push(ResolvedLineageRollout {
            plan: plan.clone(),
            history_base: info.history_base.clone(),
        });
        let Some(next) = children.get(&current) else {
            break;
        };
        current = next.clone();
        if resolved.len() > by_rollout.len() {
            anyhow::bail!("会话 {logical_thread_id} 的 rollout lineage 存在循环");
        }
    }
    if resolved.len() != by_rollout.len() {
        anyhow::bail!("会话 {logical_thread_id} 的 rollout lineage 存在断链");
    }
    Ok(resolved)
}

/// 只识别 Codex 已知的子代理父线程字段。未知形态一律按根会话校验，避免把
/// 根会话误放宽为只需要 projection state。
fn session_meta_parent_thread_id(record: &Value) -> Option<&str> {
    [
        "/payload/parent_thread_id",
        "/parent_thread_id",
        "/payload/source/subagent/thread_spawn/parent_thread_id",
        "/payload/source/subagent/parent_thread_id",
        "/payload/source/parent_thread_id",
    ]
    .into_iter()
    .find_map(|path| record.pointer(path).and_then(Value::as_str))
    .filter(|parent| !parent.trim().is_empty())
}

fn paginated_projection_is_valid(
    history: Option<&Connection>,
    info: &RolloutRecoveryInfo,
) -> anyhow::Result<bool> {
    let Some(history) = history else {
        return Ok(false);
    };
    let state = history
        .query_row(
            "SELECT next_rollout_byte_offset, next_rollout_ordinal
             FROM thread_history_projection_state WHERE thread_id=?1",
            [&info.rollout_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((next_offset, next_ordinal)) = state else {
        return Ok(false);
    };
    if next_offset < 0 || next_ordinal < 0 {
        return Ok(false);
    }
    let next_offset = u64::try_from(next_offset)?;
    let next_ordinal_u64 = u64::try_from(next_ordinal)?;
    let rollout_end_offset = *info
        .record_end_offsets
        .values()
        .last()
        .ok_or_else(|| anyhow::anyhow!("rollout 不包含记录边界"))?;
    // projection state 可以是 EOF，也可以是尚未投影完的健康前缀。前缀 cursor
    // 必须精确指向下一条真实记录的起始字节及 ordinal；等长 provider 改写不会
    // 移动该边界，Codex 可在下次启动时继续投影剩余记录。
    let cursor_matches_real_boundary = if next_offset == rollout_end_offset {
        next_ordinal_u64 == info.next_ordinal
    } else {
        info.record_start_offsets.get(&next_ordinal_u64) == Some(&next_offset)
    };
    if !cursor_matches_real_boundary {
        return Ok(false);
    }
    let item_count: i64 = history.query_row(
        "SELECT COUNT(*) FROM thread_items WHERE thread_id=?1",
        [&info.rollout_id],
        |row| row.get(0),
    )?;
    let (turn_count, max_end): (i64, Option<i64>) = history.query_row(
        "SELECT COUNT(*), MAX(rollout_end_byte_offset)
         FROM thread_turns WHERE thread_id=?1",
        [&info.rollout_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if item_count == 0 && turn_count == 0 {
        let projected_projectable_content = info
            .first_projectable_end_offset
            .is_some_and(|offset| offset <= next_offset);
        return Ok(!projected_projectable_content || info.is_subagent);
    }
    if item_count <= 0 || turn_count <= 0 {
        return Ok(false);
    }
    let invalid_turn_offset_count: i64 = history.query_row(
        "SELECT COUNT(*)
         FROM thread_turns
         WHERE thread_id=?1
           AND (
               rollout_byte_offset < 0
               OR rollout_end_byte_offset < rollout_byte_offset
               OR rollout_end_byte_offset < 0
               OR rollout_byte_offset > ?2
               OR rollout_end_byte_offset > ?2
           )",
        (&info.rollout_id, i64::try_from(next_offset)?),
        |row| row.get(0),
    )?;
    let invalid_ordinal_count: i64 = history.query_row(
        "SELECT COUNT(*)
         FROM thread_turns
         WHERE thread_id=?1
           AND (
               rollout_ordinal < 0
               OR rollout_end_ordinal < rollout_ordinal
               OR rollout_end_ordinal >= ?2
           )",
        (&info.rollout_id, next_ordinal),
        |row| row.get(0),
    )?;
    let invalid_item_ordinal_count: i64 = history.query_row(
        "SELECT COUNT(*)
         FROM thread_items
         WHERE thread_id=?1
           AND (rollout_ordinal < 0 OR rollout_ordinal >= ?2)",
        (&info.rollout_id, next_ordinal),
        |row| row.get(0),
    )?;
    // 数值范围不足以证明跳号历史中的 item 真有来源：例如 10,12 中的 11。
    let mut items =
        history.prepare("SELECT rollout_ordinal FROM thread_items WHERE thread_id=?1")?;
    let item_boundaries_match = items
        .query_map([&info.rollout_id], |row| row.get::<_, i64>(0))?
        .try_fold(true, |matches, row| -> anyhow::Result<bool> {
            Ok(matches
                && u64::try_from(row?)
                    .ok()
                    .is_some_and(|ordinal| info.record_start_offsets.contains_key(&ordinal)))
        })?;
    let mut turns = history.prepare(
        "SELECT rollout_ordinal, rollout_byte_offset, rollout_end_ordinal, rollout_end_byte_offset
         FROM thread_turns WHERE thread_id=?1",
    )?;
    let boundaries_match = turns
        .query_map([&info.rollout_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })?
        .try_fold(true, |matches, row| -> anyhow::Result<bool> {
            let (ordinal, start, end_ordinal, end) = row?;
            let (Ok(ordinal), Ok(start)) = (u64::try_from(ordinal), u64::try_from(start)) else {
                return Ok(false);
            };
            if info.record_start_offsets.get(&ordinal) != Some(&start) {
                return Ok(false);
            }
            match (end_ordinal, end) {
                (None, None) => Ok(matches),
                (Some(end_ordinal), Some(end)) => {
                    let (Ok(end_ordinal), Ok(end)) =
                        (u64::try_from(end_ordinal), u64::try_from(end))
                    else {
                        return Ok(false);
                    };
                    Ok(matches && info.record_end_offsets.get(&end_ordinal) == Some(&end))
                }
                _ => Ok(false),
            }
        })?;
    Ok(invalid_turn_offset_count == 0
        && invalid_ordinal_count == 0
        && invalid_item_ordinal_count == 0
        && item_boundaries_match
        && boundaries_match
        && max_end.is_none_or(|value| {
            value >= 0
                && u64::try_from(value).ok().is_some_and(|end| {
                    info.record_end_offsets
                        .values()
                        .any(|offset| *offset == end)
                })
        }))
}

fn mark_rollout_history_legacy(
    original: &[u8],
    expected_thread_id: &str,
) -> anyhow::Result<Vec<u8>> {
    let text = std::str::from_utf8(original)?;
    let mut output = String::with_capacity(text.len());
    let mut changed = false;
    for segment in text.split_inclusive('\n') {
        let (line, ending) = segment.strip_suffix('\n').map_or((segment, ""), |line| {
            (
                line.strip_suffix('\r').unwrap_or(line),
                if line.ends_with('\r') { "\r\n" } else { "\n" },
            )
        });
        let mut record: Value = serde_json::from_str(line)?;
        let is_target_session_meta = record.get("type").and_then(Value::as_str)
            == Some("session_meta")
            && record.pointer("/payload/id").and_then(Value::as_str) == Some(expected_thread_id);
        if is_target_session_meta {
            let payload = record
                .get_mut("payload")
                .and_then(Value::as_object_mut)
                .ok_or_else(|| anyhow::anyhow!("session_meta.payload 结构未知"))?;
            if payload
                .get("history_mode")
                .and_then(Value::as_str)
                .is_some_and(|mode| mode != "paginated")
            {
                anyhow::bail!("会话不是可恢复的 paginated 历史");
            }
            payload.insert("history_mode".into(), Value::String("legacy".into()));
            output.push_str(&serde_json::to_string(&record)?);
            output.push_str(ending);
            changed = true;
        } else {
            output.push_str(segment);
        }
    }
    if !changed {
        anyhow::bail!("未找到可恢复的 session_meta.history_mode");
    }
    Ok(output.into_bytes())
}

fn mark_database_history_legacy(
    state_path: &Path,
    plans: &[LogicalThreadHistoryRecoveryPlan],
    may_write: &mut impl FnMut() -> Result<bool, AppError>,
) -> anyhow::Result<()> {
    let mut state = Connection::open(state_path)?;
    if !table_columns(&state, "threads")?.contains("history_mode") {
        anyhow::bail!("state_5.sqlite 缺少 history_mode 列");
    }
    let transaction = state.transaction()?;
    let mut seen = BTreeSet::new();
    for plan in plans {
        if !seen.insert(&plan.logical_thread_id) {
            anyhow::bail!(
                "历史恢复计划包含重复 logical thread ID {}",
                plan.logical_thread_id
            );
        }
        if !may_write()? {
            anyhow::bail!("Codex 已重新运行或修复目标已变化，历史恢复已终止并回滚。");
        }
        let rows = transaction.execute(
            "UPDATE threads SET history_mode='legacy'
             WHERE id=?1 AND history_mode='paginated'",
            [&plan.logical_thread_id],
        )?;
        if rows != 1 {
            anyhow::bail!("会话 {} 的 history_mode 已并发变化", plan.logical_thread_id);
        }
    }
    if !may_write()? {
        anyhow::bail!("Codex 已重新运行或修复目标已变化，历史恢复已终止并回滚。");
    }
    transaction.commit()?;
    Ok(())
}

#[derive(Deserialize)]
struct MigrationReport {
    outcomes: Vec<MigrationOutcome>,
}

#[derive(Deserialize)]
struct MigrationOutcome {
    thread_id: Option<String>,
    rollout_path: PathBuf,
    status: String,
    message: Option<String>,
}

fn run_codex_history_migration(
    codex_home: &Path,
    configured_app: Option<&str>,
    plans: &[LogicalThreadHistoryRecoveryPlan],
) -> anyhow::Result<MigrationReport> {
    ensure_migration_uses_backed_up_databases(codex_home)?;
    let output = codex_history_migration_command(codex_home, configured_app, plans)?
        .output()
        .map_err(|error| anyhow::anyhow!("无法启动 Codex 历史迁移器：{error}"))?;
    if !output.status.success() {
        anyhow::bail!(
            "Codex 历史迁移器失败：{}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let report: MigrationReport = serde_json::from_slice(&output.stdout)
        .map_err(|error| anyhow::anyhow!("无法解析 Codex 历史迁移结果：{error}"))?;
    validate_migration_report(plans, &report)?;
    Ok(report)
}

fn ensure_migration_uses_backed_up_databases(codex_home: &Path) -> anyhow::Result<()> {
    if std::env::var_os("CODEX_SQLITE_HOME").is_some() {
        anyhow::bail!(
            "检测到 CODEX_SQLITE_HOME；当前恢复仅备份 CODEX_HOME 内 SQLite，拒绝向未计划数据库迁移"
        );
    }
    let config_path = codex_home.join("config.toml");
    if config_path.is_file() {
        let config = fs::read_to_string(&config_path)?;
        let document = config
            .parse::<toml_edit::DocumentMut>()
            .map_err(|error| anyhow::anyhow!("无法解析 Codex 配置中的 sqlite_home：{error}"))?;
        if toml_contains_sqlite_home(document.as_item()) {
            anyhow::bail!(
                "检测到配置 sqlite_home；当前恢复仅备份 CODEX_HOME 内 SQLite，拒绝向未计划数据库迁移"
            );
        }
    }
    let journals = codex_home.join("rollout-migrations");
    if journals.is_dir() && fs::read_dir(&journals)?.next().is_some() {
        anyhow::bail!("检测到已有 rollout-migrations journal，拒绝覆盖或清理未知迁移副产物");
    }
    Ok(())
}

fn toml_contains_sqlite_home(item: &toml_edit::Item) -> bool {
    match item {
        toml_edit::Item::Table(table) => table
            .iter()
            .any(|(key, value)| key == "sqlite_home" || toml_contains_sqlite_home(value)),
        toml_edit::Item::ArrayOfTables(tables) => tables.iter().any(|table| {
            table
                .iter()
                .any(|(key, value)| key == "sqlite_home" || toml_contains_sqlite_home(value))
        }),
        toml_edit::Item::Value(toml_edit::Value::InlineTable(table)) => {
            table.iter().any(|(key, _)| key == "sqlite_home")
        }
        _ => false,
    }
}

fn validate_migration_report(
    plans: &[LogicalThreadHistoryRecoveryPlan],
    report: &MigrationReport,
) -> anyhow::Result<()> {
    let expected = plans
        .iter()
        .flat_map(|logical_plan| {
            logical_plan
                .rollouts
                .iter()
                .map(move |rollout| (&logical_plan.logical_thread_id, rollout))
        })
        .collect::<Vec<_>>();
    if report.outcomes.len() != expected.len() {
        anyhow::bail!(
            "迁移器返回 {} 条 outcome，预期 {} 条物理 rollout outcome",
            report.outcomes.len(),
            expected.len()
        );
    }
    let mut matched = vec![false; expected.len()];
    for outcome in &report.outcomes {
        let matching = expected
            .iter()
            .enumerate()
            .filter(|(_, (logical_thread_id, rollout))| {
                outcome.thread_id.as_deref() == Some(logical_thread_id.as_str())
                    && rollout_paths_match(&outcome.rollout_path, &rollout.path)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if matching.len() != 1 || matched[matching[0]] {
            anyhow::bail!(
                "迁移器返回未知或重复 outcome：thread_id={:?}，rollout_path={}",
                outcome.thread_id,
                outcome.rollout_path.display()
            );
        }
        if outcome.status != "migrated" {
            anyhow::bail!(
                "物理 rollout {} 未完成历史迁移：{}{}",
                outcome.rollout_path.display(),
                outcome.status,
                outcome
                    .message
                    .as_deref()
                    .map(|message| format!("（{message}）"))
                    .unwrap_or_default()
            );
        }
        matched[matching[0]] = true;
    }
    if matched.iter().any(|matched| !matched) {
        anyhow::bail!("迁移器未返回所有目标物理 rollout 的 outcome");
    }
    Ok(())
}

fn rollout_paths_match(left: &Path, right: &Path) -> bool {
    fs::canonicalize(left).ok() == fs::canonicalize(right).ok()
}

fn codex_history_migration_command(
    codex_home: &Path,
    configured_app: Option<&str>,
    plans: &[LogicalThreadHistoryRecoveryPlan],
) -> anyhow::Result<std::process::Command> {
    let mut command = platform::codex_cli_command_for_app(configured_app)
        .map_err(|error| anyhow::anyhow!("无法启动 Codex 历史迁移器：{error}"))?;
    command
        .env("CODEX_HOME", codex_home)
        .arg("migrate-rollouts")
        .arg("--apply")
        .arg("--json");
    let mut seen = BTreeSet::new();
    for plan in plans {
        if seen.insert(&plan.logical_thread_id) {
            command.arg("--thread").arg(&plan.logical_thread_id);
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    Ok(command)
}

fn verify_paginated_history_recovery(
    state_path: &Path,
    history_path: &Path,
    plans: &[LogicalThreadHistoryRecoveryPlan],
    migration_report: &MigrationReport,
    target: &str,
) -> anyhow::Result<()> {
    let state = Connection::open_with_flags(
        state_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let history = Connection::open_with_flags(
        history_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    validate_migration_report(plans, migration_report)?;
    for logical_plan in plans {
        let mode: Option<String> = state
            .query_row(
                "SELECT history_mode FROM threads WHERE id=?1",
                [&logical_plan.logical_thread_id],
                |row| row.get(0),
            )
            .optional()?;
        if mode.as_deref() != Some("paginated") {
            anyhow::bail!(
                "会话 {} 迁移后 history_mode 必须为 paginated",
                logical_plan.logical_thread_id
            );
        }
        for plan in &logical_plan.rollouts {
            let bytes = fs::read(&plan.path)?;
            let info = rollout_recovery_info(&plan.path, &bytes)?;
            if info.logical_thread_id != logical_plan.logical_thread_id
                || info.rollout_id != plan.rollout_id
                || info.history_mode.as_deref() != Some("paginated")
            {
                anyhow::bail!(
                    "会话 {} 的 rollout {} 迁移后元数据不一致",
                    logical_plan.logical_thread_id,
                    plan.path.display()
                );
            }
            verify_rollout_semantic_sha256(&bytes, &plan.semantic_sha256)?;
            verify_rollout_route(&plan.path, target)?;
            if !paginated_projection_is_valid(Some(&history), &info)? {
                anyhow::bail!(
                    "会话 {} 的物理 rollout {} 分页历史投影仍不可用",
                    logical_plan.logical_thread_id,
                    plan.rollout_id
                );
            }
        }
    }
    Ok(())
}

fn entry_matches_file(entry: &SessionRepairManifestEntry, path: &Path, target: &str) -> bool {
    if entry.path.as_str() != path.to_string_lossy().as_ref()
        || entry.target != target
        || entry.provider != target
    {
        return false;
    }
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if entry.file_length != metadata.len() {
        return false;
    }
    let modified_at_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|duration| i64::try_from(duration.as_millis()).ok());
    if entry.file_modified_at_ms != modified_at_ms {
        return false;
    }
    entry.prefix_sha256.as_deref() == rollout_prefix_sha256(path).as_deref()
}

struct PlannedRollout {
    plan: RolloutPlan,
    cached_entry: Option<SessionRepairManifestEntry>,
}

fn plan_rollout(
    path: &Path,
    target: &str,
    manifest_entries: &[&SessionRepairManifestEntry],
) -> anyhow::Result<PlannedRollout> {
    if let Some(entry) = manifest_entries
        .iter()
        .copied()
        .find(|entry| entry_matches_file(entry, path, target))
    {
        return Ok(PlannedRollout {
            plan: RolloutPlan::Cached,
            cached_entry: Some(entry.clone()),
        });
    }
    let analysis = analyze_rollout_metadata_in_place(path, target)?;
    Ok(PlannedRollout {
        plan: match analysis.write {
            Some(write) => RolloutPlan::Repair {
                path: path.to_path_buf(),
                original_sha256: write.original_sha256,
                repaired_sha256: file_sha256_bytes(&write.repaired),
                meta_count: write.meta_count,
                session_meta_count: analysis.session_meta_count,
                write_mode: if !write.changes_layout {
                    RolloutWriteMode::InPlaceEqualLength
                } else {
                    RolloutWriteMode::RewriteAndReproject
                },
            },
            None => RolloutPlan::Matching {
                session_meta_count: analysis.session_meta_count,
            },
        },
        cached_entry: None,
    })
}

#[derive(Debug)]
enum RolloutPlan {
    Cached,
    Matching {
        session_meta_count: usize,
    },
    Repair {
        path: PathBuf,
        original_sha256: String,
        repaired_sha256: String,
        meta_count: usize,
        session_meta_count: usize,
        write_mode: RolloutWriteMode,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RolloutWriteMode {
    InPlaceEqualLength,
    RewriteAndReproject,
}

struct RecordedRollout {
    path: PathBuf,
    session_meta_count: usize,
}

impl RecordedRollout {
    fn to_entry(&self, target: &str) -> SessionRepairManifestEntry {
        let metadata = fs::metadata(&self.path).ok();
        let file_length = metadata.as_ref().map(|meta| meta.len()).unwrap_or(0);
        let file_modified_at_ms = metadata
            .as_ref()
            .and_then(|meta| meta.modified().ok())
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|duration| i64::try_from(duration.as_millis()).ok());
        SessionRepairManifestEntry {
            path: self.path.to_string_lossy().into_owned(),
            file_length,
            file_modified_at_ms,
            prefix_sha256: rollout_prefix_sha256(&self.path),
            provider: target.to_owned(),
            target: target.to_owned(),
            session_meta_count: self.session_meta_count,
        }
    }
}

struct BackupEntry {
    original: PathBuf,
    backup: PathBuf,
    kind: BackupEntryKind,
}

enum BackupEntryKind {
    Bytes {
        original_sha256: String,
        snapshot_sha256: String,
    },
    SqliteSnapshot {
        original_sha256: String,
        snapshot_sha256: String,
    },
    AbsentSqlite,
}

struct RepairBackup {
    dir: PathBuf,
    entries: Vec<BackupEntry>,
}

impl RepairBackup {
    fn create() -> anyhow::Result<Self> {
        let dir = std::env::temp_dir().join(format!(
            "codex-tools-session-repair-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            entries: Vec::new(),
        })
    }

    fn add_bytes(&mut self, original: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        let backup = self.dir.join(format!("{:04}.bak", self.entries.len()));
        fs::write(&backup, bytes)?;
        self.entries.push(BackupEntry {
            original: original.to_path_buf(),
            backup,
            kind: BackupEntryKind::Bytes {
                original_sha256: file_sha256_bytes(bytes),
                snapshot_sha256: file_sha256_bytes(bytes),
            },
        });
        self.write_manifest()?;
        Ok(())
    }

    /// 使用 SQLite backup API 生成逻辑一致快照。恢复时只从该快照恢复主库并移除
    /// WAL/SHM；因此验证的是快照哈希与 SQLite 完整性，不把运行时 sidecar 的
    /// 物理字节等同于一致性快照。
    fn add_sqlite_database(&mut self, original: &Path) -> anyhow::Result<()> {
        if !original.is_file() {
            let backup = self.dir.join(format!("{:04}.absent", self.entries.len()));
            self.entries.push(BackupEntry {
                original: original.to_path_buf(),
                backup,
                kind: BackupEntryKind::AbsentSqlite,
            });
            self.write_manifest()?;
            return Ok(());
        }
        let backup = self.dir.join(format!("{:04}.sqlite", self.entries.len()));
        let source = Connection::open_with_flags(
            original,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let mut snapshot = Connection::open(&backup)?;
        {
            let backup_api = Backup::new(&source, &mut snapshot)?;
            backup_api.run_to_completion(128, Duration::from_millis(5), None)?;
        }
        drop(snapshot);
        drop(source);
        let snapshot_sha256 = file_sha256(&backup)?;
        self.entries.push(BackupEntry {
            original: original.to_path_buf(),
            backup,
            kind: BackupEntryKind::SqliteSnapshot {
                original_sha256: file_sha256(original)?,
                snapshot_sha256,
            },
        });
        self.write_manifest()?;
        Ok(())
    }

    #[cfg(test)]
    fn restore(&self) -> anyhow::Result<()> {
        self.restore_selected(
            &self
                .entries
                .iter()
                .map(|entry| entry.original.clone())
                .collect(),
        )
    }

    fn restore_selected(&self, originals: &HashSet<PathBuf>) -> anyhow::Result<()> {
        let mut failures = Vec::new();
        for entry in self
            .entries
            .iter()
            .rev()
            .filter(|entry| originals.contains(&entry.original))
        {
            if let Err(error) = self.restore_entry(entry) {
                failures.push(format!("{}：{error}", entry.original.display()));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("恢复失败目标：{}", failures.join("；"))
        }
    }

    fn restore_entry(&self, entry: &BackupEntry) -> anyhow::Result<()> {
        match &entry.kind {
            BackupEntryKind::Bytes {
                snapshot_sha256, ..
            } => {
                if file_sha256(&entry.backup)? != *snapshot_sha256 {
                    anyhow::bail!("备份字节哈希核验失败");
                }
                atomic_write(&entry.original, &fs::read(&entry.backup)?)?;
                if file_sha256(&entry.original)? != *snapshot_sha256 {
                    anyhow::bail!("恢复字节哈希核验失败");
                }
            }
            BackupEntryKind::SqliteSnapshot {
                snapshot_sha256, ..
            } => {
                if file_sha256(&entry.backup)? != *snapshot_sha256 {
                    anyhow::bail!("SQLite 备份快照哈希核验失败");
                }
                let snapshot_connection = Connection::open_with_flags(
                    &entry.backup,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )?;
                let snapshot_integrity: String =
                    snapshot_connection
                        .query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
                if snapshot_integrity != "ok" {
                    anyhow::bail!("SQLite 备份 integrity_check={snapshot_integrity}");
                }
                drop(snapshot_connection);
                for suffix in ["-wal", "-shm"] {
                    let mut target = entry.original.as_os_str().to_os_string();
                    target.push(suffix);
                    let target = PathBuf::from(target);
                    if target.exists() {
                        fs::remove_file(target)?;
                    }
                }
                let snapshot = fs::read(&entry.backup)?;
                atomic_write(&entry.original, &snapshot)?;
                if file_sha256(&entry.original)? != *snapshot_sha256 {
                    anyhow::bail!("SQLite 快照哈希核验失败");
                }
                let connection = Connection::open_with_flags(
                    &entry.original,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )?;
                let integrity: String =
                    connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
                if integrity != "ok" {
                    anyhow::bail!("SQLite integrity_check={integrity}");
                }
            }
            BackupEntryKind::AbsentSqlite => {
                for suffix in ["", "-wal", "-shm"] {
                    let mut target = entry.original.as_os_str().to_os_string();
                    target.push(suffix);
                    let target = PathBuf::from(target);
                    if target.exists() {
                        fs::remove_file(target)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn write_manifest(&self) -> anyhow::Result<()> {
        let entries = self
            .entries
            .iter()
            .map(|entry| {
                let (kind, original_sha256, snapshot_sha256) = match &entry.kind {
                    BackupEntryKind::Bytes {
                        original_sha256,
                        snapshot_sha256,
                    } => ("bytes", Some(original_sha256), Some(snapshot_sha256)),
                    BackupEntryKind::SqliteSnapshot {
                        original_sha256,
                        snapshot_sha256,
                    } => (
                        "sqlite_snapshot",
                        Some(original_sha256),
                        Some(snapshot_sha256),
                    ),
                    BackupEntryKind::AbsentSqlite => ("absent_sqlite", None, None),
                };
                serde_json::json!({
                    "original_path": entry.original,
                    "backup_path": entry.backup,
                    "type": kind,
                    "original_sha256": original_sha256,
                    "snapshot_sha256": snapshot_sha256,
                })
            })
            .collect::<Vec<_>>();
        atomic_write(
            &self.dir.join("backup-manifest.json"),
            &serde_json::to_vec_pretty(&entries)?,
        )?;
        Ok(())
    }

    fn cleanup(&self) -> anyhow::Result<()> {
        fs::remove_dir_all(&self.dir)?;
        Ok(())
    }
}

fn verify_repair_commit(
    expected_rollouts: &[(PathBuf, String)],
    database_plans: &[(PathBuf, usize, String)],
    target: &str,
    scope: &SessionScope,
) -> anyhow::Result<()> {
    for (path, expected_sha256) in expected_rollouts {
        if file_sha256(path)? != *expected_sha256 {
            anyhow::bail!("会话文件 {} 在写入后发生变化", path.display());
        }
        verify_rollout_route(path, target)?;
    }
    for (path, _, _) in database_plans {
        verify_database_route(path, target, scope)?;
    }
    Ok(())
}

fn verify_rollout_route(path: &Path, target: &str) -> anyhow::Result<()> {
    let bytes = fs::read(path)?;
    let mut session_meta_count = 0;
    let mut session_meta_target_count = 0;
    for line in std::str::from_utf8(&bytes)?.lines() {
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let is_session_meta = record.get("type").and_then(Value::as_str) == Some("session_meta");
        if is_session_meta {
            session_meta_count += 1;
        }
        for (_, provider) in provider_metadata_fields(&record) {
            if provider != target {
                anyhow::bail!("会话文件 {} 仍包含旧 provider", path.display());
            }
            if is_session_meta {
                session_meta_target_count += 1;
            }
        }
    }
    if session_meta_count == 0 || session_meta_target_count == 0 {
        anyhow::bail!(
            "会话文件 {} 缺少可验证的 session_meta 目标 provider",
            path.display()
        );
    }
    Ok(())
}

/// 迁移器允许规范化 provider、history_mode、空 history_base 与 ordinal；其余 JSON
/// 记录的完整顺序、数量以及用户消息、工具调用/结果都参与哈希。
fn rollout_semantic_sha256(bytes: &[u8]) -> anyhow::Result<String> {
    let mut normalized = Vec::new();
    for segment in bytes.split_inclusive(|byte| *byte == b'\n') {
        let line = segment.strip_suffix(b"\n").unwrap_or(segment);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let mut record: Value = serde_json::from_slice(line)?;
        if let Some(object) = record.as_object_mut() {
            object.remove("ordinal");
            if object.get("type").and_then(Value::as_str) == Some("session_meta") {
                if let Some(payload) = object.get_mut("payload").and_then(Value::as_object_mut) {
                    payload.remove("model_provider");
                    payload.remove("model_provider_id");
                    payload.remove("history_mode");
                    if payload.get("history_base").is_some_and(Value::is_null) {
                        payload.remove("history_base");
                    }
                }
            } else if matches!(
                object.get("type").and_then(Value::as_str),
                Some("turn_context")
            ) {
                if let Some(payload) = object.get_mut("payload").and_then(Value::as_object_mut) {
                    payload.remove("model_provider");
                    payload.remove("model_provider_id");
                }
            } else if object.get("type").and_then(Value::as_str) == Some("event_msg")
                && object
                    .get("payload")
                    .and_then(|payload| payload.get("type"))
                    .and_then(Value::as_str)
                    == Some("thread_settings_applied")
                && let Some(settings) = object
                    .get_mut("payload")
                    .and_then(Value::as_object_mut)
                    .and_then(|payload| payload.get_mut("thread_settings"))
                    .and_then(Value::as_object_mut)
            {
                settings.remove("model_provider");
                settings.remove("model_provider_id");
            }
        }
        normalized.push(record);
    }
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&normalized)?)
    ))
}

fn verify_rollout_semantic_sha256(bytes: &[u8], expected: &str) -> anyhow::Result<()> {
    let actual = rollout_semantic_sha256(bytes)?;
    if actual != expected {
        anyhow::bail!("迁移后会话语义内容发生未允许变化");
    }
    Ok(())
}

fn verify_database_route(path: &Path, target: &str, scope: &SessionScope) -> anyhow::Result<()> {
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let Some((table, id, columns)) = session_table(&db)? else {
        anyhow::bail!("会话数据库格式在写入后发生变化");
    };
    for ids in scope.eligible_ids().chunks(900) {
        if ids.is_empty() {
            continue;
        }
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let model_violation = if columns.contains("model") {
            " OR model IS NOT NULL"
        } else {
            ""
        };
        let sql = format!(
            "SELECT COUNT(*) FROM {table} WHERE {id} IN ({placeholders}) AND (COALESCE(model_provider,'')<>?{model_violation})"
        );
        let mut params = Vec::<&dyn rusqlite::ToSql>::with_capacity(ids.len() + 1);
        params.extend(ids.iter().map(|id| id as &dyn rusqlite::ToSql));
        params.push(&target);
        let remaining: i64 = db.query_row(&sql, params.as_slice(), |row| row.get(0))?;
        if remaining != 0 {
            anyhow::bail!("会话数据库 {} 未完全写入目标路由", path.display());
        }
    }
    Ok(())
}

fn eligible_database_ids(
    db: &Connection,
    table: &str,
    id_column: &str,
    scope: &SessionScope,
) -> anyhow::Result<Vec<String>> {
    let mut output = Vec::new();
    for ids in scope.eligible_ids().chunks(900) {
        if ids.is_empty() {
            continue;
        }
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT {id_column} FROM {table} WHERE {id_column} IN ({placeholders})");
        let mut statement = db.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(ids.iter()), |row| {
            row.get::<_, String>(0)
        })?;
        output.extend(rows.flatten());
    }
    Ok(output)
}

fn eligible_database_changes(
    db: &Connection,
    table: &str,
    id_column: &str,
    target: &str,
    scope: &SessionScope,
) -> anyhow::Result<Vec<String>> {
    let mut output = Vec::new();
    let model_change = if table_columns(db, table)?.contains("model") {
        " OR model IS NOT NULL"
    } else {
        ""
    };
    for ids in scope.eligible_ids().chunks(900) {
        if ids.is_empty() {
            continue;
        }
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT {id_column} FROM {table} WHERE (COALESCE(model_provider,'')<>?1{model_change}) AND {id_column} IN ({placeholders})"
        );
        let mut params = Vec::<&dyn rusqlite::ToSql>::with_capacity(ids.len() + 1);
        params.push(&target);
        params.extend(ids.iter().map(|id| id as &dyn rusqlite::ToSql));
        let mut statement = db.prepare(&sql)?;
        let rows = statement.query_map(params.as_slice(), |row| row.get::<_, String>(0))?;
        output.extend(rows.flatten());
    }
    Ok(output)
}

fn preflight_database(path: &Path, target: &str, scope: &SessionScope) -> anyhow::Result<usize> {
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let Some((table, id, columns)) = session_table(&db)? else {
        anyhow::bail!("未知的 Codex 会话数据库结构（未找到 threads/local_thread_catalog 表）");
    };
    if !columns.contains("model_provider") {
        anyhow::bail!("会话数据库格式不受支持，缺少 model_provider 列");
    }
    Ok(eligible_database_changes(&db, table, id, target, scope)?.len())
}

fn repair_database_commit(
    path: &Path,
    target: &str,
    scope: &SessionScope,
    expected_rows: usize,
    may_write: &mut impl FnMut() -> Result<bool, AppError>,
) -> anyhow::Result<usize> {
    let mut db = Connection::open(path)?;
    let Some((table, id, columns)) = session_table(&db)? else {
        anyhow::bail!("未知的 Codex 会话数据库结构（未找到 threads/local_thread_catalog 表）");
    };
    if !columns.contains("model_provider") {
        anyhow::bail!("会话数据库格式不受支持，缺少 model_provider 列");
    }
    let ids = eligible_database_changes(&db, table, id, target, scope)?;
    let clears_model = columns.contains("model");
    let transaction = db.transaction()?;
    let mut rows = 0;
    for ids in ids.chunks(900) {
        if !may_write()? {
            anyhow::bail!("Codex 已重新运行或修复目标已变化，切换已终止并回滚。");
        }
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let model_assignment = if clears_model { ", model=NULL" } else { "" };
        let model_change = if clears_model {
            " OR model IS NOT NULL"
        } else {
            ""
        };
        let sql = format!(
            "UPDATE {table} SET model_provider=?1{model_assignment} WHERE (COALESCE(model_provider,'')<>?1{model_change}) AND {id} IN ({placeholders})"
        );
        let mut params = Vec::<&dyn rusqlite::ToSql>::with_capacity(ids.len() + 1);
        params.push(&target);
        params.extend(ids.iter().map(|id| id as &dyn rusqlite::ToSql));
        rows += transaction.execute(&sql, params.as_slice())?;
    }
    if !may_write()? {
        anyhow::bail!("Codex 已重新运行或修复目标已变化，切换已终止并回滚。");
    }
    transaction.commit()?;
    if rows != expected_rows {
        anyhow::bail!(
            "会话数据库 {} 在提交期间发生并发变化（预期更新 {expected_rows} 行，实际 {rows} 行）",
            path.display()
        );
    }
    Ok(rows)
}

/// 激活修复只处理真正保存会话的数据库。`state_5.sqlite` 是根状态库，若它存在
/// 却无法识别为会话表则必须 fail closed；sqlite 目录中的辅助库可安全跳过。
fn repair_database_paths(codex_home: &Path, paths: &[PathBuf]) -> anyhow::Result<Vec<PathBuf>> {
    let root_state = codex_home.join("state_5.sqlite");
    let mut output = Vec::new();
    for path in paths {
        let db = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        if session_table(&db)?.is_some() {
            output.push(path.clone());
        } else if path == &root_state {
            anyhow::bail!("未知的 Codex 会话数据库结构（未找到 threads/local_thread_catalog 表）");
        }
    }
    Ok(output)
}

fn repair_with_history_mode_and_guard_with_paths(
    codex_home: &Path,
    target: &str,
    _force_portable_history: bool,
    configured_app: Option<&str>,
    may_write: impl FnMut() -> Result<bool, AppError>,
) -> Result<(RepairResult, Vec<PathBuf>), AppError> {
    // 手动 IPC 与连接激活共用同一 fail-closed 原子引擎，不能让手动修复留下
    // 只改 JSONL、未同步 state/history SQLite 的半修复状态。
    repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
        codex_home,
        target,
        None,
        configured_app,
        may_write,
    )
}

struct RolloutWrite {
    original: Vec<u8>,
    repaired: Vec<u8>,
    original_sha256: String,
    meta_count: usize,
    changes_layout: bool,
}

struct RolloutAnalysis {
    session_meta_count: usize,
    write: Option<RolloutWrite>,
}

/// 流式分析一个 rollout：只读，不写盘。仅补丁确认过的 provider 值字节，绝不重序列化
/// 真实会话。长度变化由调用方检测并触发分页历史偏移的重建。
fn analyze_rollout_metadata_in_place(path: &Path, target: &str) -> anyhow::Result<RolloutAnalysis> {
    if fs::metadata(path)?.len() > MAX_REPAIR_ROLLOUT_BYTES {
        anyhow::bail!("会话文件超过 256 MB，已跳过以避免占用过多内存");
    }
    let original = fs::read(path)?;
    let target_bytes = target.as_bytes();
    let mut session_meta_count = 0;
    let mut meta_count = 0;
    let mut saw_session_meta = false;
    let mut offset = 0;
    let mut patches = Vec::new();
    let mut changes_layout = false;

    for segment in original.split_inclusive(|byte| *byte == b'\n') {
        let line_len = segment.len();
        let line = segment.strip_suffix(b"\n").unwrap_or(segment);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let record = serde_json::from_slice::<Value>(line)
            .map_err(|error| anyhow::anyhow!("未知 JSONL 结构，拒绝原位写入：{error}"))?;
        let record_type = record.get("type").and_then(Value::as_str);
        if record_type == Some("session_meta") {
            saw_session_meta = true;
            session_meta_count += 1;
            if record
                .pointer("/payload/id")
                .and_then(Value::as_str)
                .is_none()
            {
                anyhow::bail!("session_meta 缺少 id，拒绝原位写入");
            }
        }
        let known_metadata = matches!(record_type, Some("session_meta") | Some("turn_context"))
            || (record_type == Some("event_msg")
                && record.pointer("/payload/type").and_then(Value::as_str)
                    == Some("thread_settings_applied"));
        if !known_metadata {
            offset += line_len;
            continue;
        }
        // legacy rollout 的 turn_context 和 thread_settings_applied 记录经常
        // 没有 provider 字段；它们仍是合法历史，不能因为缺少可更新字段而
        // 阻断整个会话。session_meta 则必须包含 provider，才能确认路由目标。
        let fields = provider_metadata_fields(&record);
        if fields.is_empty() {
            if record_type == Some("session_meta") {
                anyhow::bail!("session_meta 缺少可原位更新的 model_provider 字段");
            }
            offset += line_len;
            continue;
        }
        if fields
            .iter()
            .map(|(_, provider)| *provider)
            .collect::<BTreeSet<_>>()
            .len()
            > 1
        {
            anyhow::bail!("会话元数据包含冲突的 provider 别名，拒绝原位写入");
        }
        for (field, provider) in fields {
            let (value_start, value_end) = unique_json_string_field_range(line, field)
                .ok_or_else(|| anyhow::anyhow!("会话元数据结构未知，拒绝原位写入"))?;
            let current = &line[value_start..value_end];
            if current != provider.as_bytes() {
                anyhow::bail!("会话元数据与原始字节不一致，拒绝原位写入");
            }
            if provider != target {
                changes_layout |= value_end - value_start != target_bytes.len();
                patches.push((offset + value_start, offset + value_end));
                meta_count += 1;
            }
        }
        offset += line_len;
    }

    if !saw_session_meta {
        anyhow::bail!("未找到可验证的 session_meta，拒绝原位写入");
    }
    let write = if meta_count > 0 {
        patches.sort_unstable_by_key(|(start, _)| *start);
        let removed = patches
            .iter()
            .map(|(start, end)| end - start)
            .sum::<usize>();
        let mut repaired = Vec::with_capacity(
            original
                .len()
                .saturating_add(patches.len().saturating_mul(target.len()))
                .saturating_sub(removed),
        );
        let mut copied = 0;
        for (start, end) in patches {
            repaired.extend_from_slice(&original[copied..start]);
            repaired.extend_from_slice(target_bytes);
            copied = end;
        }
        repaired.extend_from_slice(&original[copied..]);
        Some(RolloutWrite {
            original_sha256: {
                let mut hasher = Sha256::new();
                hasher.update(&original);
                format!("{:x}", hasher.finalize())
            },
            original,
            repaired,
            meta_count,
            changes_layout,
        })
    } else {
        None
    };
    Ok(RolloutAnalysis {
        session_meta_count,
        write,
    })
}

fn provider_metadata_fields(record: &Value) -> Vec<(&'static [u8], &str)> {
    let Some(payload) = record.get("payload") else {
        return Vec::new();
    };
    match record.get("type").and_then(Value::as_str) {
        Some("session_meta") | Some("turn_context") => ["model_provider", "model_provider_id"]
            .into_iter()
            .filter_map(|field| {
                payload
                    .get(field)
                    .and_then(Value::as_str)
                    .map(|provider| (field.as_bytes(), provider))
            })
            .collect(),
        Some("event_msg")
            if payload.get("type").and_then(Value::as_str) == Some("thread_settings_applied") =>
        {
            payload
                .get("thread_settings")
                .into_iter()
                .flat_map(|settings| {
                    ["model_provider_id", "model_provider"]
                        .into_iter()
                        .filter_map(move |field| {
                            settings
                                .get(field)
                                .and_then(Value::as_str)
                                .map(|provider| (field.as_bytes(), provider))
                        })
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Only accept a single textual occurrence of the expected key.  This avoids
/// guessing which duplicate/nested key a JSON parser selected and keeps the
/// write set limited to the provider value bytes.
fn unique_json_string_field_range(line: &[u8], field: &[u8]) -> Option<(usize, usize)> {
    let mut cursor = 0;
    let mut found = None;
    let mut key = Vec::with_capacity(field.len() + 2);
    key.push(b'"');
    key.extend_from_slice(field);
    key.push(b'"');
    while cursor + key.len() <= line.len() {
        let Some(relative) = line[cursor..]
            .windows(key.len())
            .position(|window| window == key.as_slice())
        else {
            break;
        };
        let key_start = cursor + relative;
        let mut value_start = key_start + key.len();
        while line.get(value_start).is_some_and(u8::is_ascii_whitespace) {
            value_start += 1;
        }
        if line.get(value_start) != Some(&b':') {
            cursor = key_start + key.len();
            continue;
        }
        value_start += 1;
        while line.get(value_start).is_some_and(u8::is_ascii_whitespace) {
            value_start += 1;
        }
        if line.get(value_start) != Some(&b'"') {
            cursor = key_start + key.len();
            continue;
        }
        let mut value_end = value_start + 1;
        let mut escaped = false;
        while let Some(byte) = line.get(value_end) {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                if found.replace((value_start + 1, value_end)).is_some() {
                    return None;
                }
                cursor = value_end + 1;
                break;
            }
            value_end += 1;
        }
        if value_end >= line.len() {
            return None;
        }
    }
    found
}

pub fn list_database_sessions_from_paths(
    paths: &[PathBuf],
    scope: &SessionScope,
) -> anyhow::Result<Vec<SessionSummary>> {
    let mut sessions = vec![];
    for path in paths {
        let db = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let Some((table, id, columns)) = session_table(&db)? else {
            continue;
        };
        let title = choose(&columns, &["title", "display_title"], "''");
        let provider = choose(&columns, &["model_provider"], "''");
        let cwd = choose(&columns, &["cwd"], "''");
        let archived = choose(&columns, &["archived"], "0");
        let updated = choose(&columns, &["updated_at", "source_updated_at"], "0");
        let sql = format!(
            "SELECT {id},{title},{provider},{cwd},{archived},CAST({updated} AS INTEGER) FROM {table} ORDER BY {updated} DESC LIMIT 2000"
        );
        let mut statement = db.prepare(&sql)?;
        let rows = statement.query_map([], |row| {
            let id: String = row.get(0)?;
            let archived = scope.root_archived(&id);
            let provider = normalize_provider(&row.get::<_, String>(2).unwrap_or_default());
            Ok(archived.map(|archived| SessionSummary {
                identity: format!("{}#{id}", path.display()),
                id,
                title: row.get(1).unwrap_or_default(),
                provider: provider.clone(),
                cwd: row.get(3).unwrap_or_default(),
                archived,
                updated_at: row.get(5).unwrap_or_default(),
                source_db: path.display().to_string(),
                source_rollout: None,
                original_provider: provider,
                has_user_event: false,
            }))
        })?;
        sessions.extend(rows.flatten().flatten());
    }
    Ok(sessions)
}

pub fn rollout_files(codex_home: &Path) -> Vec<PathBuf> {
    let mut output = [
        codex_home.join("sessions"),
        codex_home.join("archived_sessions"),
    ]
    .into_iter()
    .flat_map(|directory| {
        WalkDir::new(directory)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| entry.into_path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
    })
    .collect::<Vec<_>>();
    // 确定性处理顺序：修复/回滚按路径排序写入，避免不同平台文件系统迭代
    // 顺序不一致导致写序与回滚行为差异（例如外部写入目标被意外覆盖）。
    output.sort();
    output.dedup();
    output
}

pub fn database_paths(codex_home: &Path) -> Vec<PathBuf> {
    let mut output = vec![];
    let root_database = codex_home.join("state_5.sqlite");
    if root_database.is_file() {
        output.push(root_database);
    }
    collect_databases(&codex_home.join("sqlite"), &mut output);
    if let Some(path) = std::env::var_os("CODEX_SQLITE_HOME") {
        collect_databases(&PathBuf::from(path), &mut output);
    }
    if let Ok(text) = fs::read_to_string(codex_home.join("config.toml"))
        && let Ok(document) = text.parse::<toml_edit::DocumentMut>()
        && let Some(path) = document
            .get("sqlite_home")
            .and_then(toml_edit::Item::as_str)
    {
        collect_databases(&PathBuf::from(path), &mut output);
    }
    output.sort();
    output.dedup();
    output
}

fn collect_databases(path: &Path, output: &mut Vec<PathBuf>) {
    if path.is_file() {
        output.push(path.to_path_buf());
    } else if let Ok(entries) = fs::read_dir(path) {
        output.extend(entries.flatten().map(|entry| entry.path()).filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "db" || extension == "sqlite")
        }));
    }
}

#[cfg(test)]
fn repair_database(
    path: &Path,
    target: &str,
    scope: &SessionScope,
    may_write: &mut impl FnMut() -> Result<bool, AppError>,
) -> anyhow::Result<Option<usize>> {
    let mut db = Connection::open(path)?;
    let Some((table, id, columns)) = session_table(&db)? else {
        return Ok(Some(0));
    };
    if !columns.contains("model_provider") {
        return Ok(Some(0));
    }
    if !may_write()? {
        return Ok(None);
    }
    let ids = eligible_database_changes(&db, table, id, target, scope)?;
    let transaction = db.transaction()?;
    let mut rows = 0;
    for ids in ids.chunks(900) {
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "UPDATE {table} SET model_provider=?1 WHERE COALESCE(model_provider,'')<>?1 AND {id} IN ({placeholders})"
        );
        let mut params = Vec::<&dyn rusqlite::ToSql>::with_capacity(ids.len() + 1);
        params.push(&target);
        params.extend(ids.iter().map(|id| id as &dyn rusqlite::ToSql));
        rows += transaction.execute(&sql, params.as_slice())?;
    }
    if !may_write()? {
        return Ok(None);
    }
    transaction.commit()?;
    Ok(Some(rows))
}

fn inspect_database(
    path: &Path,
    scope: &SessionScope,
) -> anyhow::Result<Option<DatabaseInspection>> {
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let Some((table, id, columns)) = session_table(&db)? else {
        return Ok(None);
    };
    if !columns.contains("model_provider") {
        anyhow::bail!("会话数据库格式不受支持，无法更新归属信息");
    }
    let ids = eligible_database_ids(&db, table, id, scope)?;
    let count = ids.len() as u64;
    let mut providers = BTreeMap::<String, u64>::new();
    for ids in ids.chunks(900) {
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT model_provider, count(*) FROM {table} WHERE model_provider IS NOT NULL AND {id} IN ({placeholders}) GROUP BY model_provider"
        );
        let mut statement = db.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(ids.iter()), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;
        for (provider, count) in rows.flatten() {
            *providers.entry(provider).or_default() += count;
        }
    }
    Ok(Some(DatabaseInspection {
        schema: table.into(),
        thread_count: count,
        providers: providers.into_iter().collect(),
    }))
}

struct DatabaseInspection {
    schema: String,
    thread_count: u64,
    providers: Vec<(String, u64)>,
}

fn session_table(
    db: &Connection,
) -> anyhow::Result<Option<(&'static str, &'static str, HashSet<String>)>> {
    for (table, id) in [("threads", "id"), ("local_thread_catalog", "thread_id")] {
        let columns = table_columns(db, table)?;
        if columns.contains(id) {
            return Ok(Some((table, id, columns)));
        }
    }
    Ok(None)
}

fn table_columns(db: &Connection, table: &str) -> anyhow::Result<HashSet<String>> {
    let mut statement = db.prepare(&format!("PRAGMA table_info({table})"))?;
    Ok(statement
        .query_map([], |row| row.get::<_, String>(1))?
        .flatten()
        .collect())
}

fn choose<'a>(columns: &HashSet<String>, names: &'a [&'a str], fallback: &'a str) -> &'a str {
    names
        .iter()
        .copied()
        .find(|name| columns.contains(*name))
        .unwrap_or(fallback)
}

fn rollout_provider(path: &Path) -> anyhow::Result<Option<String>> {
    let file_size = fs::metadata(path)?.len();
    for line in std::io::BufReader::new(fs::File::open(path)?)
        .take(MAX_ROLLOUT_SCAN_BYTES)
        .lines()
    {
        let line = line?;
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if record.get("type").and_then(Value::as_str) == Some("session_meta") {
            return Ok(Some(
                record
                    .pointer("/payload/model_provider")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            ));
        }
    }
    if file_size > MAX_ROLLOUT_SCAN_BYTES {
        anyhow::bail!("前 2 MB 内没有找到会话元数据，已停止继续扫描");
    }
    Ok(None)
}

fn push_warning(warnings: &mut Vec<String>, omitted: &mut usize, warning: String) {
    if warnings.len() < MAX_REPAIR_WARNINGS.saturating_sub(1) {
        warnings.push(warning.chars().take(MAX_WARNING_CHARS).collect());
    } else {
        *omitted += 1;
    }
}

fn finish_warnings(warnings: &mut Vec<String>, omitted: usize) {
    if omitted > 0 {
        warnings.push(format!("另有 {omitted} 项警告未显示。"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufWriter, Seek, Write};

    fn single_recovery_plan(
        logical_thread_id: &str,
        rollout_id: &str,
        path: PathBuf,
        _is_subagent: bool,
    ) -> LogicalThreadHistoryRecoveryPlan {
        LogicalThreadHistoryRecoveryPlan {
            logical_thread_id: logical_thread_id.into(),
            rollouts: vec![PhysicalRolloutRecoveryPlan {
                logical_thread_id: logical_thread_id.into(),
                rollout_id: rollout_id.into(),
                path,
                original_sha256: "unused".into(),
                semantic_sha256: "unused".into(),
                recovery_reason: Some(HistoryRecoveryReason::ProjectionInvalid),
            }],
        }
    }

    #[cfg(unix)]
    fn make_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn assert_sqlite_integrity(path: &Path) {
        let database = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        let integrity: String = database
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "{}", path.display());
    }

    fn assert_no_repair_residue(home: &Path) {
        let residue = WalkDir::new(home)
            .into_iter()
            .flatten()
            .map(|entry| entry.path().to_path_buf())
            .filter(|path| {
                path.file_name().is_some_and(|name| {
                    let name = name.to_string_lossy();
                    name.contains("pending") || name.ends_with(".tmp")
                })
            })
            .collect::<Vec<_>>();
        assert!(residue.is_empty(), "遗留修复临时文件：{residue:#?}");
    }

    fn paginated_recovery_fixture(
        temp: &tempfile::TempDir,
    ) -> (PathBuf, PathBuf, PathBuf, PathBuf, String) {
        let home = temp.path().join("codex");
        let sessions = home.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let logical = "00000000-0000-7000-8000-0000000000b1".to_owned();
        let rollout = sessions.join(format!("rollout-2026-09-05T12-00-00-{logical}.jsonl"));
        fs::write(
            &rollout,
            format!(
                concat!(
                    "{{\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{logical}\",\"model_provider\":\"codex_tools_openai_relay\",\"history_mode\":\"paginated\"}}}}\n",
                    "{{\"ordinal\":1,\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_started\",\"turn_id\":\"turn-one\"}}}}\n"
                ),
                logical = logical,
            ),
        )
        .unwrap();
        let state = home.join("state_5.sqlite");
        let database = Connection::open(&state).unwrap();
        database
            .execute_batch(&format!(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT, history_mode TEXT, archived INTEGER, source TEXT);
                 INSERT INTO threads VALUES('{logical}','openai','paginated',0,NULL);"
            ))
            .unwrap();
        drop(database);
        let history = home.join(THREAD_HISTORY_FILE);
        let database = Connection::open(&history).unwrap();
        database
            .execute_batch(
                "CREATE TABLE thread_history_projection_state(
                     thread_id TEXT PRIMARY KEY,
                     next_rollout_byte_offset INTEGER,
                     next_rollout_ordinal INTEGER
                 );
                 CREATE TABLE thread_items(thread_id TEXT, rollout_ordinal INTEGER, item_json TEXT);
                 CREATE TABLE thread_turns(
                     thread_id TEXT,
                     rollout_ordinal INTEGER,
                     rollout_byte_offset INTEGER,
                     rollout_end_ordinal INTEGER,
                     rollout_end_byte_offset INTEGER
                 );",
            )
            .unwrap();
        drop(database);
        (home, rollout, state, history, logical)
    }

    fn explicit_paginated_rollout(home: &Path, logical: &str) -> PathBuf {
        let rollout = home
            .join("sessions")
            .join(format!("rollout-2026-09-05T12-00-00-{logical}.jsonl"));
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::write(
            &rollout,
            format!(
                concat!(
                    "{{\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{logical}\",\"model_provider\":\"codex_tools_openai_relay\",\"history_mode\":\"paginated\"}}}}\n",
                    "{{\"ordinal\":1,\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_started\",\"turn_id\":\"turn-one\"}}}}\n"
                ),
                logical = logical,
            ),
        )
        .unwrap();
        rollout
    }

    fn finish_synthetic_migration(
        _home: &Path,
        _configured_app: Option<&str>,
        plans: &[LogicalThreadHistoryRecoveryPlan],
    ) -> anyhow::Result<MigrationReport> {
        for logical_plan in plans {
            let state_path = logical_plan
                .rollouts
                .first()
                .and_then(|plan| plan.path.parent())
                .and_then(Path::parent)
                .expect("sessions parent")
                .join("state_5.sqlite");
            let history_path = state_path
                .parent()
                .expect("codex home")
                .join(THREAD_HISTORY_FILE);
            Connection::open(&state_path)?.execute(
                "UPDATE threads SET history_mode='paginated' WHERE id=?1",
                [&logical_plan.logical_thread_id],
            )?;
            let history = Connection::open(&history_path)?;
            for plan in &logical_plan.rollouts {
                let bytes = fs::read(&plan.path)?;
                let repaired = String::from_utf8(bytes)?.replace(
                    "\"history_mode\":\"legacy\"",
                    "\"history_mode\":\"paginated\"",
                );
                atomic_write(&plan.path, repaired.as_bytes())?;
                let info = rollout_recovery_info(&plan.path, repaired.as_bytes())?;
                let end = *info
                    .record_end_offsets
                    .values()
                    .last()
                    .expect("fixture has records");
                history.execute(
                    "INSERT OR REPLACE INTO thread_history_projection_state VALUES(?1,?2,?3)",
                    (
                        &plan.rollout_id,
                        i64::try_from(end)?,
                        i64::try_from(info.last_ordinal + 1)?,
                    ),
                )?;
                history.execute(
                    "INSERT INTO thread_items VALUES(?1,?2,'{}')",
                    (&plan.rollout_id, i64::try_from(info.last_ordinal)?),
                )?;
                history.execute(
                    "INSERT INTO thread_turns VALUES(?1,0,0,?2,?3)",
                    (
                        &plan.rollout_id,
                        i64::try_from(info.last_ordinal)?,
                        i64::try_from(end)?,
                    ),
                )?;
            }
        }
        Ok(MigrationReport {
            outcomes: plans
                .iter()
                .flat_map(|plan| {
                    plan.rollouts.iter().map(move |rollout| MigrationOutcome {
                        thread_id: Some(plan.logical_thread_id.clone()),
                        rollout_path: rollout.path.clone(),
                        status: "migrated".into(),
                        message: None,
                    })
                })
                .collect(),
        })
    }

    fn non_migrated_synthetic_report(
        _: &Path,
        _: Option<&str>,
        plans: &[LogicalThreadHistoryRecoveryPlan],
    ) -> anyhow::Result<MigrationReport> {
        Ok(MigrationReport {
            outcomes: plans
                .iter()
                .flat_map(|plan| {
                    plan.rollouts.iter().map(move |rollout| MigrationOutcome {
                        thread_id: Some(plan.logical_thread_id.clone()),
                        rollout_path: rollout.path.clone(),
                        status: "skipped".into(),
                        message: Some("injected".into()),
                    })
                })
                .collect(),
        })
    }

    fn set_session_meta_provider(path: &Path, provider: Option<&str>) -> anyhow::Result<()> {
        let mut output = String::new();
        for line in fs::read_to_string(path)?.lines() {
            let mut record: Value = serde_json::from_str(line)?;
            if record.get("type").and_then(Value::as_str) == Some("session_meta") {
                let payload = record
                    .get_mut("payload")
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| anyhow::anyhow!("fixture session_meta.payload 缺失"))?;
                match provider {
                    Some(provider) => {
                        payload.insert("model_provider".into(), Value::String(provider.into()));
                    }
                    None => {
                        payload.remove("model_provider");
                        payload.remove("model_provider_id");
                    }
                }
            }
            output.push_str(&serde_json::to_string(&record)?);
            output.push('\n');
        }
        atomic_write(path, output.as_bytes())
    }

    #[test]
    fn repair_workers_are_bounded_by_files_parallelism_and_cap() {
        assert_eq!(repair_worker_count(0, 16), 0);
        assert_eq!(repair_worker_count(2, 16), 2);
        assert_eq!(repair_worker_count(100, 2), 2);
        assert_eq!(repair_worker_count(100, 16), MAX_REPAIR_WORKERS);
    }

    #[test]
    #[ignore = "manual provider repair resource measurement"]
    fn measure_synthetic_provider_repair() {
        let mode = std::env::var("CODEX_TOOLS_PROVIDER_REPAIR_MODE")
            .expect("CODEX_TOOLS_PROVIDER_REPAIR_MODE must be setup or run");
        let scenario = std::env::var("CODEX_TOOLS_PROVIDER_REPAIR_SCENARIO")
            .expect("CODEX_TOOLS_PROVIDER_REPAIR_SCENARIO must be large or small");
        let home = PathBuf::from(
            std::env::var_os("CODEX_TOOLS_PROVIDER_REPAIR_HOME")
                .expect("CODEX_TOOLS_PROVIDER_REPAIR_HOME must be set"),
        );
        let (file_count, file_bytes) = match scenario.as_str() {
            "large" => (100, 4 * 1024 * 1024),
            "small" => (10_000, 1024),
            _ => panic!("unknown provider repair scenario: {scenario}"),
        };

        if mode == "setup" {
            assert!(!home.exists(), "fixture home must not already exist");
            let sessions = home.join("sessions");
            fs::create_dir_all(&sessions).unwrap();
            let filler = b"{\"type\":\"event_msg\",\"payload\":{\"type\":\"note\",\"message\":\"synthetic\"}}\n";
            for index in 0..file_count {
                let path = sessions.join(format!("rollout-{index:05}.jsonl"));
                let file = fs::File::create(path).unwrap();
                let mut writer = BufWriter::new(file);
                writeln!(
                    writer,
                    "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"synthetic-{index:05}\",\"model_provider\":\"openai\"}}}}"
                )
                .unwrap();
                while writer.stream_position().unwrap() < file_bytes {
                    writer.write_all(filler).unwrap();
                }
            }
            println!(
                "prepared scenario={scenario} files={file_count} target_bytes_per_file={file_bytes}"
            );
            return;
        }
        assert_eq!(mode, "run", "unknown provider repair measurement mode");

        let started = Instant::now();
        let (result, _) =
            repair_after_connection_switch_preserving_history_with_guard_at_with_paths(
                &home,
                "custom",
                Some(&home.join("measurement-manifest.json")),
                || Ok(true),
            )
            .unwrap();
        assert_eq!(result.files_scanned, file_count);
        assert_eq!(result.files_modified, file_count);
        println!(
            "measured scenario={scenario} files={file_count} engine_elapsed_ms={} wall_elapsed_ms={}",
            result.elapsed_ms,
            started.elapsed().as_millis()
        );
    }

    #[cfg(windows)]
    #[test]
    fn history_migration_command_uses_configured_desktop_cli() {
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("Custom/Codex.exe");
        let cli = temp.path().join("Custom/resources/codex.exe");
        fs::create_dir_all(cli.parent().unwrap()).unwrap();
        fs::write(&app, b"desktop").unwrap();
        fs::write(&cli, b"cli").unwrap();
        let plans = [single_recovery_plan(
            "thread-one",
            "rollout-one",
            temp.path().join("rollout.jsonl"),
            false,
        )];

        let command =
            codex_history_migration_command(temp.path(), Some(app.to_str().unwrap()), &plans)
                .unwrap();
        if let Some(configured) = std::env::var_os("CODEX_BIN") {
            assert_eq!(command.get_program(), configured);
        } else {
            assert_eq!(
                fs::canonicalize(command.get_program()).unwrap(),
                fs::canonicalize(cli).unwrap()
            );
        }
        assert!(
            command
                .get_args()
                .any(|argument| argument == "--thread" || argument == "thread-one")
        );
    }

    #[cfg(windows)]
    #[test]
    fn history_migration_command_reports_missing_configured_cli() {
        if std::env::var_os("CODEX_BIN").is_some() {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("Custom/Codex.exe");
        fs::create_dir_all(app.parent().unwrap()).unwrap();
        fs::write(&app, b"desktop").unwrap();
        let plans = [single_recovery_plan(
            "thread-one",
            "rollout-one",
            temp.path().join("rollout.jsonl"),
            false,
        )];

        let error =
            codex_history_migration_command(temp.path(), Some(app.to_str().unwrap()), &plans)
                .unwrap_err();
        assert!(error.to_string().contains("无法定位 Codex 内置 CLI"));
        assert!(error.to_string().contains("resources"));
    }

    #[test]
    fn scope_repairs_only_local_roots_and_their_descendants() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        fs::create_dir_all(home.join("archived_sessions")).unwrap();
        let database = home.join("state_5.sqlite");
        let db = Connection::open(&database).unwrap();
        db.execute_batch(
            "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT, source TEXT, archived INTEGER, title TEXT, cwd TEXT);
             CREATE TABLE local_thread_catalog(thread_id TEXT, host_id TEXT, source_kind TEXT, missing_candidate INTEGER);
             CREATE TABLE local_thread_catalog_hosts(id TEXT, host_kind TEXT);
             INSERT INTO local_thread_catalog_hosts VALUES('local-host','local'),('web-host','local');
             INSERT INTO local_thread_catalog VALUES('active','local-host','local',0),('web','web-host','chatgpt',0),('deleted','local-host','local',1);
             INSERT INTO threads VALUES
               ('active','openai',NULL,0,'active','C:/active'),
               ('active-child','openai','{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"active\"}}}',0,'child','C:/child'),
               ('archived','openai',NULL,1,'archived','C:/archived'),
               ('archived-child','openai','{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"archived\"}}}',1,'archived child','C:/archived'),
               ('web','openai','{\"source_kind\":\"chatgpt\"}',0,'web','C:/web'),
               ('deleted','openai',NULL,0,'deleted','C:/deleted'),
               ('deleted-child','openai','{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"deleted\"}}}',0,'deleted child','C:/deleted'),
               ('orphan','openai','{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"missing\"}}}',0,'orphan','C:/orphan'),
               ('cycle-a','openai','{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"cycle-b\"}}}',0,'cycle a','C:/cycle'),
               ('cycle-b','openai','{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"cycle-a\"}}}',0,'cycle b','C:/cycle');",
        )
        .unwrap();
        drop(db);

        let write_rollout = |path: &Path, id: &str, parent: Option<&str>| {
            let source = parent.map(|parent| serde_json::json!({"subagent":{"thread_spawn":{"parent_thread_id":parent}}}));
            fs::write(path, format!("{}\n", serde_json::json!({"type":"session_meta","payload":{"id":id,"model_provider":"openai","source":source}}))).unwrap();
        };
        let active = home.join("sessions/active.jsonl");
        let active_child = home.join("sessions/active-child.jsonl");
        let archived = home.join("archived_sessions/archived.jsonl");
        let web = home.join("sessions/web.jsonl");
        let deleted = home.join("sessions/deleted.jsonl");
        write_rollout(&active, "active", None);
        write_rollout(&active_child, "active-child", Some("active"));
        write_rollout(&archived, "archived", None);
        write_rollout(&web, "web", None);
        write_rollout(&deleted, "deleted", None);
        let before_web = fs::read(&web).unwrap();
        let before_deleted = fs::read(&deleted).unwrap();
        let rollouts = rollout_files(&home);
        let scope = session_scope(std::slice::from_ref(&database), &rollouts).unwrap();

        assert_eq!(scope.root_archived("active"), Some(false));
        assert_eq!(scope.root_archived("archived"), Some(true));
        assert!(scope.contains("active-child"));
        for id in [
            "web",
            "deleted",
            "deleted-child",
            "orphan",
            "cycle-a",
            "cycle-b",
        ] {
            assert!(
                !scope.contains(id),
                "{id} must remain outside the repair scope"
            );
        }

        let (result, _) =
            repair_after_connection_switch_preserving_history_with_guard_at_with_paths(
                &home,
                "custom",
                Some(&temp.path().join("manifest.json")),
                || Ok(true),
            )
            .unwrap();
        assert_eq!(result.files_modified, 3);
        assert_eq!(result.rows_updated, 4);
        assert_eq!(fs::read(&web).unwrap(), before_web);
        assert_eq!(fs::read(&deleted).unwrap(), before_deleted);
        let db = Connection::open(database).unwrap();
        let provider = |id: &str| {
            db.query_row(
                "SELECT model_provider FROM threads WHERE id=?1",
                [id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
        };
        for id in ["active", "active-child", "archived", "archived-child"] {
            assert_eq!(provider(id), "custom");
        }
        for id in ["web", "deleted"] {
            assert_eq!(provider(id), "openai");
        }
    }

    #[test]
    fn repair_unifies_all_provider_metadata_without_app_state_files() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let data = temp.path().join("data");
        fs::create_dir_all(home.join("sessions/2026")).unwrap();
        let rollout = home.join("sessions/2026/rollout.jsonl");
        let before = format!(
            "{}\n{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai","cwd":"C:/keep"}}),
            serde_json::json!({"type":"event_msg","payload":{"type":"user_message","message":"unchanged"}})
        );
        fs::write(&rollout, before).unwrap();
        let result = repair(&home, "custom").unwrap();
        assert_eq!(result.session_meta_updated, 1);
        let after = fs::read_to_string(rollout).unwrap();
        assert!(after.contains("\"model_provider\":\"custom\""));
        assert!(after.contains("\"message\":\"unchanged\""));
        assert!(!data.exists());
        assert!(!data.join("backup").exists());
    }

    #[test]
    fn repair_updates_different_length_provider_without_reserializing_history() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        let unchanged = concat!(
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"id\":\"msg-keep\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"keep exact bytes\"}]}}\r\n",
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"also keep\"}}\n"
        );
        let original = format!(
            "{{ \"type\": \"session_meta\", \"payload\": {{ \"id\": \"one\", \"model_provider\": \"other-provider\", \"future\": true }} }}\n{unchanged}"
        );
        fs::write(&rollout, &original).unwrap();

        let result = repair(&home, "custom").unwrap();

        let repaired = fs::read(&rollout).unwrap();
        assert_eq!(result.files_modified, 1);
        assert_eq!(result.session_meta_updated, 1);
        assert_eq!(
            repaired.len(),
            original.len() - ("other-provider".len() - "custom".len())
        );
        assert!(String::from_utf8_lossy(&repaired).contains("\"model_provider\": \"custom\""));
        assert!(repaired.ends_with(unchanged.as_bytes()));
    }

    #[test]
    fn repair_updates_relay_provider_in_thread_settings_without_rewriting_history() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/relay.jsonl");
        let history = concat!(
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"id\":\"msg-keep\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"keep exact history bytes\"}]}}\r\n",
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"keep following record\"}}\n"
        );
        let original = format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"relay\",\"model_provider\":\"custom\"}}}}\n{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"thread_settings_applied\",\"thread_settings\":{{\"model_provider_id\":\"codex_tools_openai_relay\"}}}}}}\n{history}"
        );
        fs::write(&rollout, &original).unwrap();

        let result = repair(&home, "custom").unwrap();

        let repaired = fs::read(&rollout).unwrap();
        assert_eq!(result.files_modified, 1);
        assert_eq!(result.session_meta_updated, 1);
        for line in repaired.split_inclusive(|byte| *byte == b'\n') {
            let line = line.strip_suffix(b"\n").unwrap_or(line);
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            assert!(!line.is_empty());
            serde_json::from_slice::<Value>(line).unwrap();
        }
        let repaired_text = String::from_utf8_lossy(&repaired);
        assert!(repaired_text.contains("\"model_provider_id\":\"custom\""));
        assert!(repaired.ends_with(history.as_bytes()));
    }

    #[test]
    fn repair_accepts_legacy_metadata_without_provider_on_context_records() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/legacy.jsonl");
        let original = concat!(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"legacy\",\"model_provider\":\"custom\",\"history_mode\":\"legacy\"}}\n",
            "{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"turn-1\",\"model\":\"custom-model\"}}\n",
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"thread_settings_applied\",\"thread_settings\":{\"model\":\"custom-model\"}}}\n",
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"id\":\"msg-keep\",\"content\":[]}}\n"
        );
        fs::write(&rollout, original).unwrap();

        let result = repair(&home, "openai").unwrap();

        assert_eq!(result.files_modified, 1);
        assert_eq!(result.session_meta_updated, 1);
        let repaired = fs::read(&rollout).unwrap();
        assert_eq!(repaired.len(), original.len());
        assert!(String::from_utf8_lossy(&repaired).contains("\"model_provider\":\"openai\""));
        assert!(String::from_utf8_lossy(&repaired).contains("\"model\":\"custom-model\""));
        assert!(String::from_utf8_lossy(&repaired).contains("msg-keep"));
    }

    #[test]
    fn scan_reports_per_provider_counts() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        fs::write(
            home.join("sessions/openai.jsonl"),
            format!(
                "{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":"openai","model_provider":"openai"}})
            ),
        )
        .unwrap();
        fs::write(
            home.join("sessions/custom.jsonl"),
            format!(
                "{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":"custom","model_provider":"custom"}})
            ),
        )
        .unwrap();
        let db = home.join("state_5.sqlite");
        let connection = Connection::open(&db).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT); INSERT INTO threads VALUES('one','openai'); INSERT INTO threads VALUES('two','openai'); INSERT INTO threads VALUES('three','custom');",
            )
            .unwrap();
        drop(connection);

        let result = scan(&home);

        assert_eq!(result.session_meta_count, 2);
        assert_eq!(result.rollout_files, 2);
        assert_eq!(result.databases[0].thread_count, 3);
        let by_id: BTreeMap<_, _> = result
            .targets
            .iter()
            .map(|target| (target.id.as_str(), target))
            .collect();
        assert_eq!(by_id["openai"].count, 3);
        assert!(by_id["openai"].sources.contains(&"sqlite".to_string()));
        assert!(by_id["openai"].sources.contains(&"rollout".to_string()));
        assert_eq!(by_id["custom"].count, 2);
        assert!(by_id["custom"].sources.contains(&"sqlite".to_string()));
        assert!(by_id["custom"].sources.contains(&"rollout".to_string()));
    }

    #[test]
    fn repair_preserves_already_matching_metadata_byte_for_byte() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        let unchanged = r#"{ "type": "session_meta", "payload": { "id": "two", "model_provider": "custom", "future": true } }"#;
        let original = format!(
            "{}\n{unchanged}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai"}})
        );
        fs::write(&rollout, &original).unwrap();
        let outcome = repair(&home, "custom").unwrap();
        assert_eq!(outcome.session_meta_updated, 1);
        let repaired = fs::read_to_string(rollout).unwrap();
        assert!(repaired.ends_with(&format!("{unchanged}\n")));
    }

    #[test]
    fn repair_reports_guard_conflict_as_skipped_without_writing() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        let original = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"guard-conflict","model_provider":"openai"}})
        );
        fs::write(&rollout, &original).unwrap();

        let error = repair_with_guard(&home, "custom", || Ok(false)).unwrap_err();

        assert!(error.to_string().contains("切换已终止"));
        assert_eq!(fs::read_to_string(rollout).unwrap(), original);
    }

    #[test]
    fn repair_preserves_a_rollout_changed_after_preflight() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        fs::write(
            &rollout,
            format!(
                "{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":"concurrent","model_provider":"openai"}})
            ),
        )
        .unwrap();
        let concurrent = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"concurrent","model_provider":"openai","external":true}})
        );
        let mut guard_calls = 0;

        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || {
                guard_calls += 1;
                if guard_calls == 1 {
                    fs::write(&rollout, &concurrent).unwrap();
                }
                Ok(true)
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("正在更新会话"));
        assert_eq!(fs::read_to_string(rollout).unwrap(), concurrent);
    }

    #[test]
    fn sqlite_update_is_narrow_and_transactional() {
        let temp = tempfile::tempdir().unwrap();
        let db = temp.path().join("state.sqlite");
        let connection = Connection::open(&db).unwrap();
        connection.execute_batch("CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT, title TEXT); INSERT INTO threads VALUES('one','other-provider','keep'); INSERT INTO threads VALUES('two','custom','same'); INSERT INTO threads VALUES('three',NULL,'missing');").unwrap();
        drop(connection);
        assert_eq!(
            repair_database(
                &db,
                "custom",
                &session_scope(std::slice::from_ref(&db), &[]).unwrap(),
                &mut || Ok(true),
            )
            .unwrap(),
            Some(2)
        );
        let connection = Connection::open(db).unwrap();
        let providers = connection
            .prepare("SELECT model_provider FROM threads ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(providers, vec!["custom", "custom", "custom"]);
        let title: String = connection
            .query_row("SELECT title FROM threads WHERE id='one'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(title, "keep");
    }

    #[test]
    fn repair_toggles_all_metadata_between_managed_providers() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        fs::write(
            &rollout,
            format!(
                "{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai"}})
            ),
        )
        .unwrap();
        let to_custom = repair(&home, "custom").unwrap();
        assert_eq!(to_custom.session_meta_updated, 1);
        assert!(
            fs::read_to_string(&rollout)
                .unwrap()
                .contains("\"model_provider\":\"custom\"")
        );

        let to_openai = repair(&home, "openai").unwrap();
        assert_eq!(to_openai.session_meta_updated, 1);
        assert!(
            fs::read_to_string(rollout)
                .unwrap()
                .contains("\"model_provider\":\"openai\"")
        );
    }

    #[test]
    fn provider_change_preserves_response_items_and_ids_byte_for_byte() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        let records = [
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"custom"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"reasoning","id":"msg_wrong-for-reasoning","summary":[]}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","id":"msg_other-provider","role":"assistant","content":[{"type":"output_text","text":"keep text"}]}}),
            serde_json::json!({"type":"response_item","payload":{"type":"function_call","id":"msg_wrong-for-call","call_id":"call_keep","name":"exec","arguments":"{}"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"function_call_output","id":"fco_local","call_id":"call_keep","output":"keep output"}}),
        ];
        fs::write(
            &rollout,
            records
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();
        let before = fs::read(&rollout).unwrap();

        let result = repair(&home, "openai").unwrap();

        assert_eq!(result.files_modified, 1);
        let repaired = fs::read(&rollout).unwrap();
        assert_eq!(repaired.len(), before.len());
        let repaired = String::from_utf8(repaired).unwrap();
        assert!(repaired.contains("msg_wrong-for-reasoning"));
        assert!(repaired.contains("msg_other-provider"));
        assert!(repaired.contains("msg_wrong-for-call"));
        assert!(repaired.contains("keep text"));
        assert!(repaired.contains("\"call_id\":\"call_keep\""));
        assert!(repaired.contains("\"id\":\"fco_local\""));
        assert!(repaired.contains("keep output"));
    }

    #[test]
    fn third_party_to_third_party_switch_portabilizes_same_provider_family() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        fs::write(
            &rollout,
            format!(
                "{}\n{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"custom"}}),
                serde_json::json!({"type":"response_item","payload":{"type":"function_call","id":"msg_provider-a","call_id":"call_keep","name":"exec","arguments":"{}"}})
            ),
        )
        .unwrap();
        let before = fs::read(&rollout).unwrap();
        let normal = repair(&home, "custom").unwrap();
        assert_eq!(normal.files_modified, 0);
        assert_eq!(fs::read(&rollout).unwrap(), before);

        let switched = repair_after_connection_switch(&home, "custom").unwrap();
        assert_eq!(switched.files_modified, 0);
        let repaired = fs::read_to_string(&rollout).unwrap();
        assert!(repaired.contains("msg_provider-a"));
        assert!(repaired.contains("\"call_id\":\"call_keep\""));
    }

    #[test]
    fn activation_repair_updates_provider_in_place_and_preserves_rollout_layout() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        let original =
            br#"{ "type": "session_meta", "payload": { "id": "one", "model_provider": "openai" } }
{"type":"turn_context","payload":{"model_provider":"openai"}}
{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model_provider_id":"openai"}}}
{"type":"event_msg","payload":{"type":"user_message","message":"keep exact bytes except provider"}}
"#;
        fs::write(&rollout, original).unwrap();
        let database = home.join("state_5.sqlite");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT, title TEXT);
                 INSERT INTO threads VALUES('one','openai','keep title');",
            )
            .unwrap();
        drop(connection);

        let result =
            repair_after_connection_switch_preserving_history_with_guard(&home, "custom", || {
                Ok(true)
            })
            .unwrap();

        assert_eq!(result.files_scanned, 1);
        assert_eq!(result.files_modified, 1);
        assert_eq!(result.session_meta_updated, 3);
        assert_eq!(result.rows_updated, 1);
        let repaired = fs::read(&rollout).unwrap();
        assert_eq!(repaired.len(), original.len());
        assert!(String::from_utf8_lossy(&repaired).contains("\"model_provider\": \"custom\""));
        assert!(String::from_utf8_lossy(&repaired).contains("\"model_provider\":\"custom\""));
        assert!(String::from_utf8_lossy(&repaired).contains("\"model_provider_id\":\"custom\""));
        assert!(String::from_utf8_lossy(&repaired).contains("keep exact bytes except provider"));
        let connection = Connection::open(database).unwrap();
        let provider: String = connection
            .query_row(
                "SELECT model_provider FROM threads WHERE id='one'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(provider, "custom");
        let title: String = connection
            .query_row("SELECT title FROM threads WHERE id='one'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(title, "keep title");
    }

    #[test]
    fn repair_rejects_unmanaged_provider_targets() {
        let temp = tempfile::tempdir().unwrap();
        let error = repair(temp.path(), "third-party").unwrap_err();
        assert!(error.to_string().contains("只能在 OpenAI"));
    }

    #[test]
    fn warning_lists_are_bounded_and_report_omissions() {
        let mut warnings = Vec::new();
        let mut omitted = 0;
        for index in 0..150 {
            push_warning(&mut warnings, &mut omitted, format!("warning-{index}"));
        }
        finish_warnings(&mut warnings, omitted);

        assert_eq!(warnings.len(), MAX_REPAIR_WARNINGS);
        assert_eq!(warnings.last().unwrap(), "另有 51 项警告未显示。");
    }

    #[test]
    fn incremental_switch_caches_unchanged_rollouts_from_manifest() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let manifest_path = temp.path().join("manifest.json");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        fs::write(
            &rollout,
            format!(
                "{}\n{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"custom"}}),
                serde_json::json!({"type":"turn_context","payload":{"model_provider":"custom"}})
            ),
        )
        .unwrap();

        let first = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&manifest_path),
            || Ok(true),
        )
        .unwrap();
        assert_eq!(first.files_opened, 1);
        assert_eq!(first.files_skipped, 1);
        assert_eq!(first.files_cached, 0);
        assert!(first.repair_complete);
        assert!(manifest_path.exists());

        let second = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&manifest_path),
            || Ok(true),
        )
        .unwrap();
        assert_eq!(second.files_cached, 1);
        assert_eq!(second.files_opened, 0);
        assert_eq!(second.files_modified, 0);
        assert!(second.repair_complete);
    }

    #[test]
    fn incremental_switch_flips_provider_and_rebuilds_manifest() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let manifest_path = temp.path().join("manifest.json");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        fs::write(
            &rollout,
            format!(
                "{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai"}})
            ),
        )
        .unwrap();

        let to_custom = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&manifest_path),
            || Ok(true),
        )
        .unwrap();
        assert_eq!(to_custom.files_modified, 1);
        assert_eq!(to_custom.files_cached, 0);

        let to_custom_again = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&manifest_path),
            || Ok(true),
        )
        .unwrap();
        assert_eq!(to_custom_again.files_cached, 1);
        assert_eq!(to_custom_again.files_opened, 0);

        let to_openai = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "openai",
            Some(&manifest_path),
            || Ok(true),
        )
        .unwrap();
        assert_eq!(to_openai.files_modified, 1);
        assert_eq!(to_openai.files_cached, 0);
        assert!(
            fs::read_to_string(&rollout)
                .unwrap()
                .contains("\"model_provider\":\"openai\"")
        );
    }

    #[test]
    fn unknown_database_schema_aborts_switch_before_write() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let manifest_path = temp.path().join("manifest.json");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        let original = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai"}})
        );
        fs::write(&rollout, &original).unwrap();
        let db = home.join("state_5.sqlite");
        let connection = Connection::open(&db).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE unrelated(id TEXT PRIMARY KEY); INSERT INTO unrelated VALUES('x');",
            )
            .unwrap();
        drop(connection);
        let db_before = fs::read(&db).unwrap();

        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&manifest_path),
            || Ok(true),
        )
        .unwrap_err();

        assert!(error.to_string().contains("未知的 Codex 会话数据库结构"));
        assert_eq!(fs::read_to_string(&rollout).unwrap(), original);
        assert_eq!(fs::read(&db).unwrap(), db_before);
        assert!(!manifest_path.exists());
    }

    #[test]
    fn explicit_paginated_length_change_rejects_configured_external_sqlite_without_home_state() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let logical = "00000000-0000-7000-8000-0000000000c1";
        let rollout = explicit_paginated_rollout(&home, logical);
        let external = temp.path().join("isolated-sqlite");
        fs::create_dir_all(&external).unwrap();
        let external_db = external.join("outside.sqlite");
        Connection::open(&external_db)
            .unwrap()
            .execute_batch(
                "CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES('keep');",
            )
            .unwrap();
        let config = home.join("config.toml");
        fs::write(
            &config,
            format!(
                "sqlite_home = {}\n",
                serde_json::to_string(&external.to_string_lossy().to_string()).unwrap()
            ),
        )
        .unwrap();
        let rollout_before = fs::read(&rollout).unwrap();
        let external_before = fs::read(&external_db).unwrap();

        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || Ok(true),
        )
        .unwrap_err();

        assert!(error.to_string().contains("sqlite_home"));
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
        assert_eq!(fs::read(&external_db).unwrap(), external_before);
    }

    #[test]
    fn missing_mode_length_change_rejects_configured_external_sqlite_without_home_state() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let logical = "00000000-0000-7000-8000-0000000000c5";
        let rollout = home.join(format!(
            "sessions/rollout-2026-09-05T12-00-00-{logical}.jsonl"
        ));
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::write(
            &rollout,
            format!(
                "{{\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{logical}\",\"model_provider\":\"codex_tools_openai_relay\"}}}}\n"
            ),
        )
        .unwrap();
        let external = temp.path().join("isolated-sqlite");
        fs::create_dir_all(&external).unwrap();
        let external_db = external.join("outside.sqlite");
        Connection::open(&external_db)
            .unwrap()
            .execute_batch(
                "CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES('keep');",
            )
            .unwrap();
        fs::write(
            home.join("config.toml"),
            format!(
                "sqlite_home = {}\n",
                serde_json::to_string(&external.to_string_lossy().to_string()).unwrap()
            ),
        )
        .unwrap();
        let rollout_before = fs::read(&rollout).unwrap();
        let external_before = fs::read(&external_db).unwrap();

        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || Ok(true),
        )
        .unwrap_err();

        assert!(error.to_string().contains("sqlite_home"));
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
        assert_eq!(fs::read(&external_db).unwrap(), external_before);
    }

    #[test]
    fn explicit_paginated_length_change_rejects_missing_state_before_write() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let rollout = explicit_paginated_rollout(&home, "00000000-0000-7000-8000-0000000000c2");
        let rollout_before = fs::read(&rollout).unwrap();

        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || Ok(true),
        )
        .unwrap_err();

        assert!(error.to_string().contains("需要 state_5.sqlite"));
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
    }

    #[test]
    fn explicit_paginated_length_change_rejects_state_missing_history_mode_column_before_write() {
        let temp = tempfile::tempdir().unwrap();
        let (home, rollout, state, history, _logical) = paginated_recovery_fixture(&temp);
        fs::remove_file(&state).unwrap();
        Connection::open(&state)
            .unwrap()
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT);
                 INSERT INTO threads VALUES('00000000-0000-7000-8000-0000000000b1','openai');",
            )
            .unwrap();
        let rollout_before = fs::read(&rollout).unwrap();
        let state_before = fs::read(&state).unwrap();
        let history_before = fs::read(&history).unwrap();

        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || Ok(true),
        )
        .unwrap_err();

        assert!(error.to_string().contains("id/history_mode"));
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
        assert_eq!(fs::read(&state).unwrap(), state_before);
        assert_eq!(fs::read(&history).unwrap(), history_before);
    }

    #[test]
    fn explicit_paginated_length_change_rejects_state_mode_mismatch_before_write() {
        let temp = tempfile::tempdir().unwrap();
        let (home, rollout, state, history, logical) = paginated_recovery_fixture(&temp);
        Connection::open(&state)
            .unwrap()
            .execute(
                "UPDATE threads SET history_mode='legacy' WHERE id=?1",
                [&logical],
            )
            .unwrap();
        let rollout_before = fs::read(&rollout).unwrap();
        let state_before = fs::read(&state).unwrap();
        let history_before = fs::read(&history).unwrap();

        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || Ok(true),
        )
        .unwrap_err();

        assert!(error.to_string().contains("分页状态不一致"));
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
        assert_eq!(fs::read(&state).unwrap(), state_before);
        assert_eq!(fs::read(&history).unwrap(), history_before);
    }

    #[test]
    fn legacy_rollout_with_paginated_state_is_rejected_before_length_change() {
        let temp = tempfile::tempdir().unwrap();
        let (home, rollout, state, history, logical) = paginated_recovery_fixture(&temp);
        let legacy = format!(
            concat!(
                "{{\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{logical}\",\"model_provider\":\"codex_tools_openai_relay\",\"history_mode\":\"legacy\"}}}}\n",
                "{{\"ordinal\":1,\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_started\",\"turn_id\":\"turn-one\"}}}}\n"
            ),
            logical = logical,
        );
        fs::write(&rollout, &legacy).unwrap();
        let rollout_before = fs::read(&rollout).unwrap();
        let state_before = fs::read(&state).unwrap();
        let history_before = fs::read(&history).unwrap();

        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || Ok(true),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("legacy history_mode 与分页证据冲突")
        );
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
        assert_eq!(fs::read(&state).unwrap(), state_before);
        assert_eq!(fs::read(&history).unwrap(), history_before);
    }

    #[test]
    fn explicit_legacy_length_change_without_projection_remains_supported() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/legacy.jsonl");
        fs::write(
            &rollout,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"legacy\",\"model_provider\":\"codex_tools_openai_relay\",\"history_mode\":\"legacy\"}}\n",
        )
        .unwrap();

        let result = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || Ok(true),
        )
        .unwrap();

        assert_eq!(result.files_modified, 1);
        assert!(
            fs::read_to_string(&rollout)
                .unwrap()
                .contains("\"model_provider\":\"custom\"")
        );
    }

    #[test]
    fn activation_repair_ignores_auxiliary_sqlite_databases_without_session_tables() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let manifest_path = temp.path().join("manifest.json");
        fs::create_dir_all(home.join("sessions")).unwrap();
        fs::create_dir_all(home.join("sqlite")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        fs::write(
            &rollout,
            format!(
                "{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":"active","model_provider":"openai"}})
            ),
        )
        .unwrap();
        let session_db = home.join("sqlite/codex-dev.db");
        let db = Connection::open(&session_db).unwrap();
        db.execute_batch(
            "CREATE TABLE local_thread_catalog(thread_id TEXT PRIMARY KEY, host_id TEXT, source_kind TEXT, missing_candidate INTEGER, model_provider TEXT);
             CREATE TABLE local_thread_catalog_hosts(id TEXT PRIMARY KEY, host_kind TEXT);
             INSERT INTO local_thread_catalog_hosts VALUES('host','local');
             INSERT INTO local_thread_catalog VALUES('active','host','local',0,'openai');",
        )
        .unwrap();
        drop(db);
        let auxiliary = home.join("sqlite/codex-history-snapshots-dev.db");
        let db = Connection::open(&auxiliary).unwrap();
        db.execute_batch("CREATE TABLE snapshots(id TEXT PRIMARY KEY, payload TEXT); INSERT INTO snapshots VALUES('keep','unchanged');").unwrap();
        drop(db);
        let auxiliary_before = fs::read(&auxiliary).unwrap();

        let result = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&manifest_path),
            || Ok(true),
        )
        .unwrap();

        assert_eq!(result.databases_scanned, 1);
        assert_eq!(result.databases_updated, 1);
        assert_eq!(result.rows_updated, 1);
        assert_eq!(fs::read(&auxiliary).unwrap(), auxiliary_before);
        let db = Connection::open(session_db).unwrap();
        let provider: String = db
            .query_row(
                "SELECT model_provider FROM local_thread_catalog WHERE thread_id='active'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(provider, "custom");
    }

    #[test]
    fn failed_switch_rolls_back_rollout_writes() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let manifest_path = temp.path().join("manifest.json");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        let original = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai"}})
        );
        fs::write(&rollout, &original).unwrap();
        let db = home.join("state_5.sqlite");
        let connection = Connection::open(&db).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT); INSERT INTO threads VALUES('one','openai');",
            )
            .unwrap();
        drop(connection);
        let mut guard_calls = 0;
        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&manifest_path),
            || {
                guard_calls += 1;
                Ok(guard_calls < 3)
            },
        )
        .unwrap_err();

        // 调用顺序：1 预检门禁；2 rollout 写入；3 数据库门禁（返回 false 触发回滚）
        assert_eq!(guard_calls, 3);
        assert!(error.to_string().contains("已终止并回滚"));
        assert_eq!(fs::read_to_string(&rollout).unwrap(), original);
        let connection = Connection::open_with_flags(
            &db,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        let provider: String = connection
            .query_row(
                "SELECT model_provider FROM threads WHERE id='one'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let integrity: String = connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(provider, "openai");
        assert_eq!(integrity, "ok");
        assert!(!manifest_path.exists());
    }

    #[test]
    fn database_phase_failure_restores_rollout_and_database() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let manifest_path = temp.path().join("manifest.json");
        fs::create_dir_all(home.join("sessions")).unwrap();
        fs::create_dir_all(home.join("sqlite")).unwrap();
        let rollout = home.join("sessions/rollout.jsonl");
        let original = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai"}})
        );
        fs::write(&rollout, &original).unwrap();
        let db_a = home.join("sqlite/a.sqlite");
        let db_b = home.join("sqlite/b.sqlite");
        for db_path in [&db_a, &db_b] {
            let connection = Connection::open(db_path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT); INSERT INTO threads VALUES('one','openai');",
                )
                .unwrap();
            drop(connection);
        }
        let mut guard_calls = 0;
        let error = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&manifest_path),
            || {
                guard_calls += 1;
                Ok(guard_calls < 4)
            },
        )
        .unwrap_err();

        // 调用顺序：1 预检门禁；2 rollout；3 数据库 a；4 数据库 b（返回 false 触发回滚）
        assert_eq!(guard_calls, 4);
        assert!(error.to_string().contains("已终止并回滚"));
        assert_eq!(fs::read_to_string(&rollout).unwrap(), original);
        for path in [&db_a, &db_b] {
            let connection = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap();
            let provider: String = connection
                .query_row(
                    "SELECT model_provider FROM threads WHERE id='one'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let integrity: String = connection
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .unwrap();
            assert_eq!(provider, "openai");
            assert_eq!(integrity, "ok");
        }
        assert!(!manifest_path.exists());
    }

    #[test]
    fn backup_phase_conflict_preserves_external_changes_without_rollback() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let sessions = home.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let first = sessions.join("a.jsonl");
        let second = sessions.join("b.jsonl");
        let original = |id| {
            format!(
                "{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":id,"model_provider":"openai"}})
            )
        };
        fs::write(&first, original("one")).unwrap();
        fs::write(&second, original("two")).unwrap();
        let first_external = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai","external":"first"}})
        );
        let second_external = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"two","model_provider":"openai","external":"second"}})
        );
        let mut hook = |stage| {
            if stage == RepairTestStage::AfterBackup(first.clone()) {
                fs::write(&first, &first_external)?;
                fs::write(&second, &second_external)?;
            }
            Ok(())
        };
        let error = repair_after_connection_switch_preserving_history_with_test_hooks(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            None,
            || Ok(true),
            &mut hook,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("写入前预检或备份失败"));
        assert_eq!(fs::read_to_string(&first).unwrap(), first_external);
        assert_eq!(fs::read_to_string(&second).unwrap(), second_external);
        assert_no_repair_residue(&home);
    }

    #[test]
    fn rollback_restores_only_written_rollout_and_preserves_external_unwritten_target() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let sessions = home.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let first = sessions.join("a.jsonl");
        let second = sessions.join("b.jsonl");
        let first_original = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai"}})
        );
        let second_original = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"two","model_provider":"openai"}})
        );
        fs::write(&first, &first_original).unwrap();
        fs::write(&second, &second_original).unwrap();
        let second_external = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"two","model_provider":"openai","external":true}})
        );
        let mut hook = |stage| {
            if stage == RepairTestStage::AfterRolloutWrite(first.clone()) {
                fs::write(&second, &second_external)?;
            }
            Ok(())
        };
        let error = repair_after_connection_switch_preserving_history_with_test_hooks(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            None,
            || Ok(true),
            &mut hook,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("已回滚全部修改"));
        assert_eq!(fs::read_to_string(&first).unwrap(), first_original);
        assert_eq!(fs::read_to_string(&second).unwrap(), second_external);
        assert_no_repair_residue(&home);
    }

    #[test]
    fn rollback_after_history_mode_commit_restores_rollout_and_sqlite() {
        let temp = tempfile::tempdir().unwrap();
        let (home, rollout, state, history, logical) = paginated_recovery_fixture(&temp);
        let rollout_before = fs::read(&rollout).unwrap();
        let mut hook = |stage| match stage {
            RepairTestStage::AfterHistoryModeCommitted => anyhow::bail!("注入 state 提交后失败"),
            _ => Ok(()),
        };
        let error = repair_after_connection_switch_preserving_history_with_test_hooks(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            None,
            || Ok(true),
            &mut hook,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("已回滚全部修改"));
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
        let route: (String, String) = Connection::open(&state)
            .unwrap()
            .query_row(
                "SELECT model_provider, history_mode FROM threads WHERE id=?1",
                [&logical],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(route, ("openai".into(), "paginated".into()));
        assert_sqlite_integrity(&state);
        assert_sqlite_integrity(&history);
        assert_no_repair_residue(&home);
    }

    #[test]
    fn migration_cli_start_failure_rolls_back_all_mutations() {
        let temp = tempfile::tempdir().unwrap();
        let (home, rollout, state, history, logical) = paginated_recovery_fixture(&temp);
        let rollout_before = fs::read(&rollout).unwrap();
        let mut hook = |_| Ok(());
        let error = repair_after_connection_switch_preserving_history_with_test_hooks(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            Some(&temp.path().join("missing/Codex.exe").to_string_lossy()),
            || Ok(true),
            &mut hook,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("无法启动 Codex 历史迁移器"));
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
        let route: (String, String) = Connection::open(&state)
            .unwrap()
            .query_row(
                "SELECT model_provider, history_mode FROM threads WHERE id=?1",
                [&logical],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(route, ("openai".into(), "paginated".into()));
        assert_sqlite_integrity(&state);
        assert_sqlite_integrity(&history);
        assert_no_repair_residue(&home);
    }

    #[test]
    fn migration_non_migrated_outcome_rolls_back_all_mutations() {
        let temp = tempfile::tempdir().unwrap();
        let (home, rollout, state, history, logical) = paginated_recovery_fixture(&temp);
        let rollout_before = fs::read(&rollout).unwrap();
        let mut hook = |_| Ok(());
        let error = repair_after_connection_switch_preserving_history_with_test_hooks(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            None,
            || Ok(true),
            &mut hook,
            Some(non_migrated_synthetic_report),
        )
        .unwrap_err();

        assert!(error.to_string().contains("未完成历史迁移"));
        assert_eq!(fs::read(&rollout).unwrap(), rollout_before);
        let route: (String, String) = Connection::open(&state)
            .unwrap()
            .query_row(
                "SELECT model_provider, history_mode FROM threads WHERE id=?1",
                [&logical],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(route, ("openai".into(), "paginated".into()));
        assert_sqlite_integrity(&state);
        assert_sqlite_integrity(&history);
        assert_no_repair_residue(&home);
    }

    #[test]
    fn migration_postcondition_failures_roll_back_all_mutations() {
        for fault in [
            "legacy",
            "provider",
            "provider_missing",
            "provider_null",
            "cursor",
            "semantic",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (home, rollout, state, history, logical) = paginated_recovery_fixture(&temp);
            let rollout_before = fs::read(&rollout).unwrap();
            let pending = home
                .join("rollout-migrations")
                .join(format!("{logical}.pending"));
            let temporary = rollout.with_file_name(format!(
                ".{}.paginated.tmp",
                rollout.file_name().unwrap().to_string_lossy()
            ));
            let mut hook = |stage| {
                if stage == RepairTestStage::AfterMigration {
                    fs::create_dir_all(pending.parent().unwrap())?;
                    fs::write(&pending, b"current migration pending")?;
                    fs::write(&temporary, b"current migration temporary")?;
                    match fault {
                        "legacy" => {
                            let text = fs::read_to_string(&rollout)?;
                            atomic_write(
                                &rollout,
                                text.replace(
                                    "\"history_mode\":\"paginated\"",
                                    "\"history_mode\":\"legacy\"",
                                )
                                .as_bytes(),
                            )?;
                        }
                        "provider" => {
                            set_session_meta_provider(&rollout, Some("openai"))?;
                        }
                        "provider_missing" => {
                            set_session_meta_provider(&rollout, None)?;
                        }
                        "provider_null" => {
                            let mut output = String::new();
                            for line in fs::read_to_string(&rollout)?.lines() {
                                let mut record: Value = serde_json::from_str(line)?;
                                if record.get("type").and_then(Value::as_str)
                                    == Some("session_meta")
                                {
                                    *record
                                        .pointer_mut("/payload/model_provider")
                                        .expect("synthetic session provider") = Value::Null;
                                }
                                output.push_str(&serde_json::to_string(&record)?);
                                output.push('\n');
                            }
                            atomic_write(&rollout, output.as_bytes())?;
                        }
                        "cursor" => {
                            Connection::open(&history)?.execute(
                                "UPDATE thread_history_projection_state SET next_rollout_byte_offset=0",
                                [],
                            )?;
                        }
                        "semantic" => {
                            let text = fs::read_to_string(&rollout)?;
                            atomic_write(
                                &rollout,
                                text.replace("turn-one", "tampered-turn").as_bytes(),
                            )?;
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(())
            };
            let result = repair_after_connection_switch_preserving_history_with_test_hooks(
                &home,
                "custom",
                Some(&temp.path().join("manifest.json")),
                None,
                || Ok(true),
                &mut hook,
                Some(finish_synthetic_migration),
            );
            assert!(
                result.is_err(),
                "{fault} unexpectedly succeeded: {result:#?}"
            );
            let error = result.unwrap_err();

            assert!(
                error.to_string().contains("已回滚全部修改"),
                "{fault}: {error}"
            );
            assert_eq!(fs::read(&rollout).unwrap(), rollout_before, "{fault}");
            let route: (String, String) = Connection::open(&state)
                .unwrap()
                .query_row(
                    "SELECT model_provider, history_mode FROM threads WHERE id=?1",
                    [&logical],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(route, ("openai".into(), "paginated".into()), "{fault}");
            let rows: i64 = Connection::open(&history)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM thread_history_projection_state",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(rows, 0, "{fault}");
            assert_sqlite_integrity(&state);
            assert_sqlite_integrity(&history);
            assert!(!pending.exists(), "{fault}");
            assert!(!temporary.exists(), "{fault}");
            assert_no_repair_residue(&home);
        }
    }

    #[test]
    fn preexisting_unknown_migration_residue_is_rejected_and_preserved() {
        let temp = tempfile::tempdir().unwrap();
        let (home, _rollout, _state, _history, _logical) = paginated_recovery_fixture(&temp);
        let journals = home.join("rollout-migrations");
        fs::create_dir_all(&journals).unwrap();
        let unknown = journals.join("previous-run.pending");
        let bytes = b"unknown previous migration";
        fs::write(&unknown, bytes).unwrap();
        let mut hook = |_| Ok(());
        let error = repair_after_connection_switch_preserving_history_with_test_hooks(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            None,
            || Ok(true),
            &mut hook,
            None,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("已有 rollout-migrations journal")
        );
        assert_eq!(fs::read(&unknown).unwrap(), bytes);
    }

    #[test]
    fn provider_database_commit_failure_rolls_back_rollout_and_database() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/one.jsonl");
        let rollout_before = format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":"one","model_provider":"openai"}})
        );
        fs::write(&rollout, &rollout_before).unwrap();
        let state = home.join("state_5.sqlite");
        Connection::open(&state)
            .unwrap()
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT);
                 INSERT INTO threads VALUES('one','openai');",
            )
            .unwrap();
        let mut hook = |stage| match stage {
            RepairTestStage::BeforeDatabaseCommit(_) => {
                anyhow::bail!("注入 provider 数据库提交失败")
            }
            _ => Ok(()),
        };
        let error = repair_after_connection_switch_preserving_history_with_test_hooks(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            None,
            || Ok(true),
            &mut hook,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("已回滚全部修改"));
        assert_eq!(fs::read_to_string(&rollout).unwrap(), rollout_before);
        let provider: String = Connection::open(&state)
            .unwrap()
            .query_row(
                "SELECT model_provider FROM threads WHERE id='one'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(provider, "openai");
        assert_sqlite_integrity(&state);
        assert_no_repair_residue(&home);
    }

    #[test]
    fn malformed_history_database_is_never_touched() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/one.jsonl");
        let original = concat!(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"one\",\"model_provider\":\"openai\"}}\n",
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"reasoning\",\"id\":\"rs-old\"}}\n"
        );
        fs::write(&rollout, original).unwrap();
        let state = home.join("state_5.sqlite");
        let db = Connection::open(&state).unwrap();
        db.execute_batch(
            "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT, model TEXT);
             INSERT INTO threads VALUES('one','openai','old');",
        )
        .unwrap();
        drop(db);
        let state_before = fs::read(&state).unwrap();
        let history = Connection::open(home.join(THREAD_HISTORY_FILE)).unwrap();
        history
            .execute_batch(
                "CREATE TABLE thread_history_projection_state(thread_id TEXT);
                 CREATE TABLE thread_items(thread_id TEXT, turn_id TEXT, item_id TEXT, rollout_ordinal INTEGER, item_json TEXT);
                 CREATE TABLE thread_turns(thread_id TEXT, turn_id TEXT, rollout_ordinal INTEGER, rollout_byte_offset INTEGER, rollout_end_ordinal INTEGER, rollout_end_byte_offset INTEGER);",
            )
            .unwrap();
        drop(history);
        let history_before = fs::read(home.join(THREAD_HISTORY_FILE)).unwrap();

        let result = repair_after_connection_switch_preserving_history_with_guard_at(
            &home,
            "custom",
            Some(&temp.path().join("manifest.json")),
            || Ok(true),
        )
        .unwrap();
        assert!(result.repair_complete);
        assert_eq!(fs::read_to_string(&rollout).unwrap().len(), original.len());
        assert!(
            fs::read_to_string(&rollout)
                .unwrap()
                .contains("\"model_provider\":\"custom\"")
        );
        assert_ne!(fs::read(&state).unwrap(), state_before);
        let db = Connection::open(state).unwrap();
        let route: (String, Option<String>) = db
            .query_row(
                "SELECT model_provider, model FROM threads WHERE id='one'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(route, ("custom".into(), None));
        assert_eq!(
            fs::read(home.join(THREAD_HISTORY_FILE)).unwrap(),
            history_before
        );
    }

    #[test]
    fn legacy_recovery_marker_preserves_every_non_session_record_byte_for_byte() {
        let response = concat!(
            "{\"ordinal\":1,\"type\":\"response_item\",\"payload\":{\"type\":\"reasoning\",\"id\":\"rs-keep\"}}\r\n",
            "{\"ordinal\":2,\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"keep context\"}]}}\n"
        );
        let original = format!(
            "{}\n{response}",
            serde_json::json!({
                "ordinal": 0,
                "type": "session_meta",
                "payload": {
                    "id": "one",
                    "model_provider": "custom",
                    "history_mode": "paginated"
                }
            })
        );

        let recovered = mark_rollout_history_legacy(original.as_bytes(), "one").unwrap();
        let recovered = String::from_utf8(recovered).unwrap();

        assert!(recovered.ends_with(response));
        let session: Value = serde_json::from_str(recovered.lines().next().unwrap()).unwrap();
        assert_eq!(
            session
                .pointer("/payload/history_mode")
                .and_then(Value::as_str),
            Some("legacy")
        );
        assert_eq!(session.get("ordinal").and_then(Value::as_i64), Some(0));
        assert!(recovered.contains("rs-keep"));
        assert!(recovered.contains("keep context"));
    }

    #[test]
    fn metadata_summary_allows_legacy_without_ordinal_but_strict_paginated_parser_rejects_it() {
        let logical = "00000000-0000-7000-8000-00000000000a";
        let legacy = format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{logical}\",\"history_mode\":\"legacy\"}}}}\n"
        );
        assert_eq!(
            rollout_metadata_summary(legacy.as_bytes())
                .unwrap()
                .history_mode
                .as_deref(),
            Some("legacy")
        );
        let path = PathBuf::from(format!("rollout-2026-09-05T12-00-00-{logical}.jsonl"));
        assert!(rollout_recovery_info(&path, legacy.as_bytes()).is_err());
    }

    // 真实问题是 10→12 跳号。末尾附加记录保证 turn 的 inclusive end 不等于 EOF。
    fn t7_gapped_projection_fixture(
        temp: &tempfile::TempDir,
        provider: &str,
    ) -> (PathBuf, PathBuf, PathBuf, PathBuf, String) {
        let fixture = paginated_recovery_fixture(temp);
        let (_, rollout, state_path, history_path, logical) = &fixture;
        let lines = [
            format!("{{\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{logical}\",\"model_provider\":\"{provider}\",\"history_mode\":\"paginated\"}}}}\n"),
            "{\"ordinal\":10,\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-one\"}}\n".into(),
            "{\"ordinal\":12,\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-one\"}}\n".into(),
            "{\"ordinal\":13,\"type\":\"turn_context\",\"payload\":{\"note\":\"after turn\"}}\n".into(),
        ];
        fs::write(rollout, lines.concat()).unwrap();
        Connection::open(state_path)
            .unwrap()
            .execute("UPDATE threads SET model_provider=?1", [provider])
            .unwrap();
        let history = Connection::open(history_path).unwrap();
        history
            .execute(
                "INSERT INTO thread_history_projection_state VALUES(?1,?2,14)",
                (logical, lines.iter().map(String::len).sum::<usize>() as i64),
            )
            .unwrap();
        history
            .execute("INSERT INTO thread_items VALUES(?1,10,'{}')", [logical])
            .unwrap();
        history
            .execute(
                "INSERT INTO thread_turns VALUES(?1,10,?2,12,?3)",
                (
                    logical,
                    lines[0].len() as i64,
                    lines[..3].iter().map(String::len).sum::<usize>() as i64,
                ),
            )
            .unwrap();
        fixture
    }

    #[test]
    fn t7_healthy_gapped_projection_equal_length_switch_never_migrates() {
        for (provider, target) in [("custom", "openai"), ("openai", "custom")] {
            let temp = tempfile::tempdir().unwrap();
            let (home, rollout, state, history, _) = t7_gapped_projection_fixture(&temp, provider);
            let before = fs::read_to_string(&rollout).unwrap();
            let history_before = fs::read(&history).unwrap();
            let info = rollout_recovery_info(&rollout, before.as_bytes()).unwrap();
            assert!(
                paginated_projection_is_valid(Some(&Connection::open(&history).unwrap()), &info)
                    .unwrap()
            );
            let mut hook = |stage| {
                assert_ne!(stage, RepairTestStage::AfterHistoryModeCommitted);
                Ok(())
            };
            fn no_migration(
                _: &Path,
                _: Option<&str>,
                _: &[LogicalThreadHistoryRecoveryPlan],
            ) -> anyhow::Result<MigrationReport> {
                panic!("健康跳号历史的等长切换不能启动迁移器")
            }
            let (result, _) = repair_after_connection_switch_preserving_history_with_test_hooks(
                &home,
                target,
                Some(&temp.path().join("manifest.json")),
                None,
                || Ok(true),
                &mut hook,
                Some(no_migration),
            )
            .unwrap();
            assert!(result.repair_complete);
            assert_eq!(result.files_modified, 1);
            assert_eq!(
                fs::read_to_string(&rollout).unwrap(),
                before.replace(provider, target)
            );
            assert_eq!(fs::read(&history).unwrap(), history_before);
            let route: (String, String) = Connection::open(&state)
                .unwrap()
                .query_row(
                    "SELECT model_provider, history_mode FROM threads",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(route, (target.into(), "paginated".into()));
        }
    }

    #[test]
    fn healthy_projection_prefix_equal_length_switch_never_migrates() {
        for (provider, target) in [("custom", "openai"), ("openai", "custom")] {
            let temp = tempfile::tempdir().unwrap();
            let (home, rollout, state, history_path, logical) =
                t7_gapped_projection_fixture(&temp, provider);
            let before = fs::read_to_string(&rollout).unwrap();
            let record_starts = before
                .split_inclusive('\n')
                .scan(0_u64, |offset, line| {
                    let current = *offset;
                    *offset += line.len() as u64;
                    Some(current)
                })
                .collect::<Vec<_>>();
            let history = Connection::open(&history_path).unwrap();
            history
                .execute(
                    "UPDATE thread_history_projection_state
                     SET next_rollout_byte_offset=?2, next_rollout_ordinal=13
                     WHERE thread_id=?1",
                    (logical.as_str(), record_starts[3] as i64),
                )
                .unwrap();
            drop(history);
            let history_before = fs::read(&history_path).unwrap();
            let info = rollout_recovery_info(&rollout, before.as_bytes()).unwrap();
            assert!(
                paginated_projection_is_valid(
                    Some(&Connection::open(&history_path).unwrap()),
                    &info,
                )
                .unwrap()
            );
            fn no_migration(
                _: &Path,
                _: Option<&str>,
                _: &[LogicalThreadHistoryRecoveryPlan],
            ) -> anyhow::Result<MigrationReport> {
                panic!("健康 projection 前缀的等长切换不能启动迁移器")
            }
            let (result, _) = repair_after_connection_switch_preserving_history_with_test_hooks(
                &home,
                target,
                Some(&temp.path().join("manifest.json")),
                None,
                || Ok(true),
                &mut |_| Ok(()),
                Some(no_migration),
            )
            .unwrap();
            assert!(result.repair_complete);
            assert_eq!(
                fs::read_to_string(&rollout).unwrap(),
                before.replace(provider, target)
            );
            assert_eq!(fs::read(&history_path).unwrap(), history_before);
            let route: (String, String) = Connection::open(&state)
                .unwrap()
                .query_row(
                    "SELECT model_provider, history_mode FROM threads",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(route, (target.into(), "paginated".into()));
        }
    }

    #[test]
    fn equal_length_switch_preserves_preexisting_projection_without_migration() {
        for fault in [
            "cursor_eof",
            "cursor_ordinal",
            "item_gap",
            "turn_gap",
            "turn_offset",
            "null_ordinal",
            "null_offset",
            "missing_projection",
        ] {
            for (provider, target) in [("custom", "openai"), ("openai", "custom")] {
                let temp = tempfile::tempdir().unwrap();
                let (home, rollout, state, history_path, _) =
                    t7_gapped_projection_fixture(&temp, provider);
                let history = Connection::open(&history_path).unwrap();
                let sql = match fault {
                    "cursor_eof" => {
                        "UPDATE thread_history_projection_state SET next_rollout_byte_offset=next_rollout_byte_offset-1"
                    }
                    "cursor_ordinal" => {
                        "UPDATE thread_history_projection_state SET next_rollout_ordinal=13"
                    }
                    "item_gap" => "UPDATE thread_items SET rollout_ordinal=11",
                    "turn_gap" => "UPDATE thread_turns SET rollout_end_ordinal=11",
                    "turn_offset" => {
                        "UPDATE thread_turns SET rollout_end_byte_offset=rollout_end_byte_offset-1"
                    }
                    "null_ordinal" => "UPDATE thread_turns SET rollout_end_ordinal=NULL",
                    "null_offset" => "UPDATE thread_turns SET rollout_end_byte_offset=NULL",
                    _ => "DELETE FROM thread_history_projection_state",
                };
                history.execute(sql, []).unwrap();
                drop(history);
                let rollout_before = fs::read_to_string(&rollout).unwrap();
                let history_before = fs::read(&history_path).unwrap();
                fn no_migration(
                    _: &Path,
                    _: Option<&str>,
                    _: &[LogicalThreadHistoryRecoveryPlan],
                ) -> anyhow::Result<MigrationReport> {
                    panic!("等长 provider 改写不能修复或迁移既有 projection")
                }
                let (result, _) =
                    repair_after_connection_switch_preserving_history_with_test_hooks(
                        &home,
                        target,
                        Some(&temp.path().join("manifest.json")),
                        None,
                        || Ok(true),
                        &mut |_| Ok(()),
                        Some(no_migration),
                    )
                    .unwrap();
                assert!(result.repair_complete, "{fault}");
                assert_eq!(
                    fs::read_to_string(&rollout).unwrap(),
                    rollout_before.replace(provider, target),
                    "{fault}"
                );
                assert_eq!(fs::read(&history_path).unwrap(), history_before, "{fault}");
                let route: (String, String) = Connection::open(&state)
                    .unwrap()
                    .query_row(
                        "SELECT model_provider, history_mode FROM threads",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .unwrap();
                assert_eq!(route, (target.into(), "paginated".into()), "{fault}");
            }
        }
    }

    #[test]
    fn inherited_session_meta_uses_only_the_first_record_as_rollout_identity() {
        let temp = tempfile::tempdir().unwrap();
        let (home, rollout, _state, history_path, logical) = paginated_recovery_fixture(&temp);
        let parent = "00000000-0000-7000-8000-0000000000b2";
        let ancestor = "00000000-0000-7000-8000-0000000000b3";
        let lines = [
            format!(
                "{{\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{logical}\",\"session_id\":\"{parent}\",\"parent_thread_id\":\"{parent}\",\"model_provider\":\"openai\",\"history_mode\":\"paginated\"}}}}\n"
            ),
            format!(
                "{{\"ordinal\":1,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{parent}\",\"model_provider\":\"openai\",\"history_mode\":\"legacy\",\"history_base\":{{\"thread_id\":\"{ancestor}\",\"end_ordinal_exclusive\":50,\"end_byte_offset\":500}}}}}}\n"
            ),
            "{\"ordinal\":2,\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-one\"}}\n".into(),
            "{\"ordinal\":3,\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-one\"}}\n".into(),
            "{\"ordinal\":4,\"type\":\"world_state\",\"payload\":{\"state\":{}}}\n".into(),
        ];
        let original = lines.concat();
        fs::write(&rollout, &original).unwrap();
        let history = Connection::open(&history_path).unwrap();
        history
            .execute(
                "INSERT INTO thread_history_projection_state VALUES(?1,?2,5)",
                (logical.as_str(), original.len() as i64),
            )
            .unwrap();
        history
            .execute(
                "INSERT INTO thread_items VALUES(?1,2,'{}')",
                [logical.as_str()],
            )
            .unwrap();
        history
            .execute(
                "INSERT INTO thread_turns VALUES(?1,2,?2,3,?3)",
                (
                    logical.as_str(),
                    (lines[0].len() + lines[1].len()) as i64,
                    lines[..4].iter().map(String::len).sum::<usize>() as i64,
                ),
            )
            .unwrap();
        drop(history);

        let info = rollout_recovery_info(&rollout, original.as_bytes()).unwrap();
        assert_eq!(info.logical_thread_id, logical);
        assert!(info.is_subagent);
        assert_eq!(info.history_mode.as_deref(), Some("paginated"));
        assert!(info.history_base.is_none());
        assert_eq!(info.record_start_offsets.len(), 5);
        assert!(
            paginated_projection_is_valid(Some(&Connection::open(&history_path).unwrap()), &info)
                .unwrap()
        );

        let mut scope = SessionScope::default();
        scope.eligible.insert(logical.clone());
        scope.eligible_rollouts.insert(rollout.clone());
        assert!(
            preflight_paginated_history_recovery(
                &home,
                &scope,
                std::slice::from_ref(&rollout),
                &HashSet::new(),
            )
            .unwrap()
            .is_empty()
        );

        let analysis = analyze_rollout_metadata_in_place(&rollout, "custom").unwrap();
        assert_eq!(analysis.session_meta_count, 2);
        let write = analysis.write.unwrap();
        assert!(!write.changes_layout);
        assert_eq!(write.meta_count, 2);
        let repaired = String::from_utf8(write.repaired).unwrap();
        assert_eq!(repaired.matches("\"model_provider\":\"custom\"").count(), 2);
        assert!(repaired.contains(&format!("\"id\":\"{logical}\"")));
        assert!(repaired.contains(&format!("\"id\":\"{parent}\"")));

        let invalid = original.replacen(&format!("\"id\":\"{logical}\","), "", 1);
        let error = rollout_recovery_info(&rollout, invalid.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(error.contains("首条 session_meta 缺少 logical thread ID"));
    }

    #[test]
    fn t7_legacy_and_undeclared_ordinals_do_not_require_lineage() {
        for mode in [Some("legacy"), None] {
            for missing in [true, false] {
                for target in ["custom", "openai"] {
                    let temp = tempfile::tempdir().unwrap();
                    let (home, rollout, state, history, _) = paginated_recovery_fixture(&temp);
                    let original = fs::read_to_string(&rollout).unwrap().replace(
                        ",\"history_mode\":\"paginated\"",
                        &mode
                            .map(|mode| format!(",\"history_mode\":\"{mode}\""))
                            .unwrap_or_default(),
                    );
                    let original = if missing {
                        original
                            .replace("\"ordinal\":0,", "")
                            .replace("\"ordinal\":1,", "")
                    } else {
                        original
                            .replace("\"ordinal\":0", "\"ordinal\":10")
                            .replace("\"ordinal\":1,", "\"ordinal\":12,")
                    };
                    fs::write(&rollout, &original).unwrap();
                    Connection::open(&state)
                        .unwrap()
                        .execute("UPDATE threads SET history_mode='legacy'", [])
                        .unwrap();
                    let history_before = fs::read(&history).unwrap();
                    let result = repair_after_connection_switch_preserving_history_with_guard_at(
                        &home,
                        target,
                        Some(&temp.path().join("manifest.json")),
                        || Ok(true),
                    )
                    .unwrap();
                    assert!(result.repair_complete);
                    assert_eq!(
                        fs::read_to_string(&rollout).unwrap(),
                        original.replace("codex_tools_openai_relay", target)
                    );
                    assert_eq!(fs::read(&history).unwrap(), history_before);
                }
            }
        }
    }

    #[test]
    fn t7_gapped_length_changing_reprojection_fails_before_any_write() {
        for fault in [
            "length",
            "cursor_eof",
            "cursor_ordinal",
            "item_gap",
            "turn_gap",
            "turn_offset",
            "null_ordinal",
            "null_offset",
            "missing_projection",
        ] {
            for target in ["custom", "openai"] {
                let temp = tempfile::tempdir().unwrap();
                let provider = "codex_tools_openai_relay";
                let (home, rollout, state, history_path, _) =
                    t7_gapped_projection_fixture(&temp, provider);
                let history = Connection::open(&history_path).unwrap();
                let sql = match fault {
                    "length" => None,
                    "cursor_eof" => Some(
                        "UPDATE thread_history_projection_state SET next_rollout_byte_offset=next_rollout_byte_offset-1",
                    ),
                    "cursor_ordinal" => {
                        Some("UPDATE thread_history_projection_state SET next_rollout_ordinal=13")
                    }
                    "item_gap" => Some("UPDATE thread_items SET rollout_ordinal=11"),
                    "turn_gap" => Some("UPDATE thread_turns SET rollout_end_ordinal=11"),
                    "turn_offset" => Some(
                        "UPDATE thread_turns SET rollout_end_byte_offset=rollout_end_byte_offset-1",
                    ),
                    "null_ordinal" => Some("UPDATE thread_turns SET rollout_end_ordinal=NULL"),
                    "null_offset" => Some("UPDATE thread_turns SET rollout_end_byte_offset=NULL"),
                    _ => Some("DELETE FROM thread_history_projection_state"),
                };
                if let Some(sql) = sql {
                    history.execute(sql, []).unwrap();
                }
                drop(history);
                let before = [&rollout, &state, &history_path].map(|path| fs::read(path).unwrap());
                let mut hook = |_| -> anyhow::Result<()> {
                    panic!("{fault}: 写入前不能进入备份或提交阶段")
                };
                let error = repair_after_connection_switch_preserving_history_with_test_hooks(
                    &home,
                    target,
                    Some(&temp.path().join("manifest.json")),
                    None,
                    || Ok(true),
                    &mut hook,
                    Some(finish_synthetic_migration),
                )
                .unwrap_err()
                .to_string();
                assert!(
                    error.contains("缺号")
                        && error.contains("写入前")
                        && error.contains(rollout.to_str().unwrap()),
                    "{fault}: {error}"
                );
                assert_eq!(
                    [&rollout, &state, &history_path].map(|path| fs::read(path).unwrap()),
                    before,
                    "{fault}"
                );
                assert_no_repair_residue(&home);
            }
        }
    }

    #[test]
    fn t7_length_changing_paginated_illegal_ordinals_fail_before_any_write() {
        for ordinal in [
            Some("10"),
            Some("9"),
            Some("-1"),
            Some("\"12\""),
            Some("null"),
            Some("12.5"),
            Some("9223372036854775807"),
            None,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (home, rollout, state, history, _) =
                t7_gapped_projection_fixture(&temp, "codex_tools_openai_relay");
            let original = fs::read_to_string(&rollout).unwrap();
            let invalid = original.replace(
                "\"ordinal\":12,",
                &ordinal
                    .map(|value| format!("\"ordinal\":{value},"))
                    .unwrap_or_default(),
            );
            fs::write(&rollout, invalid).unwrap();
            let before = [&rollout, &state, &history].map(|path| fs::read(path).unwrap());
            let mut hook =
                |_| -> anyhow::Result<()> { panic!("非法 ordinal 不能进入写入阶段") };
            let error = repair_after_connection_switch_preserving_history_with_test_hooks(
                &home,
                "custom",
                Some(&temp.path().join("manifest.json")),
                None,
                || Ok(true),
                &mut hook,
                Some(finish_synthetic_migration),
            )
            .unwrap_err()
            .to_string();
            assert!(
                error.contains("ordinal") && error.contains(rollout.to_str().unwrap()),
                "{error}"
            );
            assert_eq!(
                [&rollout, &state, &history].map(|path| fs::read(path).unwrap()),
                before
            );
            assert_no_repair_residue(&home);
        }
    }

    #[test]
    fn t7_in_progress_turn_requires_paired_null_boundaries() {
        let temp = tempfile::tempdir().unwrap();
        let (_, rollout, _, history, _) = t7_gapped_projection_fixture(&temp, "custom");
        let info = rollout_recovery_info(&rollout, &fs::read(&rollout).unwrap()).unwrap();
        let history = Connection::open(history).unwrap();
        history
            .execute(
                "UPDATE thread_turns SET rollout_end_ordinal=NULL, rollout_end_byte_offset=NULL",
                [],
            )
            .unwrap();
        assert!(paginated_projection_is_valid(Some(&history), &info).unwrap());
    }

    #[test]
    fn repair_updates_all_known_provider_aliases_and_rejects_conflicts() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        fs::create_dir_all(home.join("sessions")).unwrap();
        let rollout = home.join("sessions/aliases.jsonl");
        fs::write(
            &rollout,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"one\",\"model_provider\":\"codex_tools_openai_relay\",\"model_provider_id\":\"codex_tools_openai_relay\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"thread_settings_applied\",\"thread_settings\":{\"model_provider\":\"codex_tools_openai_relay\",\"model_provider_id\":\"codex_tools_openai_relay\"}}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"codex_tools_openai_relay must remain body text\"}]}}\n"
            ),
        )
        .unwrap();
        repair(&home, "custom").unwrap();
        let repaired = fs::read_to_string(&rollout).unwrap();
        assert!(!repaired.contains("\"model_provider\":\"codex_tools_openai_relay\""));
        assert!(!repaired.contains("\"model_provider_id\":\"codex_tools_openai_relay\""));
        assert!(repaired.contains("body text"));

        let conflict = home.join("sessions/conflict.jsonl");
        let original = "{\"type\":\"session_meta\",\"payload\":{\"id\":\"two\",\"model_provider\":\"openai\",\"model_provider_id\":\"custom\"}}\n";
        fs::write(&conflict, original).unwrap();
        assert!(repair(&home, "custom").is_err());
        assert_eq!(fs::read_to_string(&conflict).unwrap(), original);
    }

    #[test]
    fn migration_semantic_hash_rejects_tampered_user_or_tool_content() {
        let original = concat!(
            "{\"timestamp\":\"2026-09-05T12:00:00.000Z\",\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{\"id\":\"00000000-0000-7000-8000-0000000000ae\",\"model_provider\":\"codex_tools_openai_relay\",\"history_mode\":\"legacy\"}}\n",
            "{\"timestamp\":\"2026-09-05T12:00:00.000Z\",\"ordinal\":1,\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"semantic user body\"}}\n",
            "{\"timestamp\":\"2026-09-05T12:00:00.000Z\",\"ordinal\":2,\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"call-one\",\"output\":\"semantic tool body\"}}\n"
        );
        let hash = rollout_semantic_sha256(original.as_bytes()).unwrap();
        let allowed_only = original
            .replace("codex_tools_openai_relay", "custom")
            .replace(
                "\"history_mode\":\"legacy\"",
                "\"history_mode\":\"paginated\"",
            );
        verify_rollout_semantic_sha256(allowed_only.as_bytes(), &hash).unwrap();
        let null_history_base = allowed_only.replace(
            "\"history_mode\":\"paginated\"",
            "\"history_mode\":\"paginated\",\"history_base\":null",
        );
        verify_rollout_semantic_sha256(null_history_base.as_bytes(), &hash).unwrap();
        assert!(
            verify_rollout_semantic_sha256(
                original
                    .replace("semantic user body", "tampered user body")
                    .as_bytes(),
                &hash
            )
            .is_err()
        );
        assert!(
            verify_rollout_semantic_sha256(
                original
                    .replace("semantic tool body", "tampered tool body")
                    .as_bytes(),
                &hash
            )
            .is_err()
        );
        assert!(
            verify_rollout_semantic_sha256(
                original
                    .replace(
                        "\"history_mode\":\"legacy\"",
                        "\"history_mode\":\"paginated\",\"history_base\":{\"thread_id\":\"00000000-0000-7000-8000-0000000000af\",\"end_ordinal_exclusive\":1,\"end_byte_offset\":1}",
                    )
                    .as_bytes(),
                &hash
            )
                .is_err()
        );
        let with_base = original.replace(
            "\"history_mode\":\"legacy\"",
            "\"history_mode\":\"legacy\",\"history_base\":{\"thread_id\":\"00000000-0000-7000-8000-0000000000af\",\"end_ordinal_exclusive\":1,\"end_byte_offset\":1}",
        );
        let with_base_hash = rollout_semantic_sha256(with_base.as_bytes()).unwrap();
        for altered in [
            with_base.replace(
                "\"history_base\":{\"thread_id\":\"00000000-0000-7000-8000-0000000000af\",\"end_ordinal_exclusive\":1,\"end_byte_offset\":1}",
                "\"history_base\":null",
            ),
            with_base.replace(
                "\"history_base\":{\"thread_id\":\"00000000-0000-7000-8000-0000000000af\",\"end_ordinal_exclusive\":1,\"end_byte_offset\":1}",
                "",
            ),
            with_base.replace("\"end_byte_offset\":1", "\"end_byte_offset\":2"),
        ] {
            assert!(
                verify_rollout_semantic_sha256(altered.as_bytes(), &with_base_hash).is_err(),
                "非空 history_base 的删除或修改必须改变语义哈希"
            );
        }
    }

    #[test]
    #[ignore = "requires CODEX_TOOLS_TEST_CODEX_CLI and an explicit isolated official CLI run"]
    fn official_cli_single_rollout_rewrites_provider_and_reprojects() {
        let cli = PathBuf::from(
            std::env::var_os("CODEX_TOOLS_TEST_CODEX_CLI")
                .expect("CODEX_TOOLS_TEST_CODEX_CLI must name the official Codex CLI"),
        );
        assert!(cli.is_file(), "官方 CLI 不可用");
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let logical = "00000000-0000-7000-8000-0000000000ac";
        let rollout = home.join(format!(
            "sessions/2026/09/05/rollout-2026-09-05T12-00-00-{logical}.jsonl"
        ));
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        let stamp = "2026-09-05T12:00:00.000Z";
        fs::write(
            &rollout,
            format!(
                concat!(
                    "{{\"timestamp\":\"{}\",\"ordinal\":0,\"type\":\"session_meta\",\"payload\":{{\"id\":\"{}\",\"session_id\":\"{}\",\"timestamp\":\"{}\",\"cwd\":\"D:\\\\synthetic\",\"originator\":\"codex-tools-test\",\"cli_version\":\"0.153.0-alpha.5\",\"source\":\"cli\",\"model_provider\":\"codex_tools_openai_relay\",\"history_mode\":\"legacy\"}}}}\n",
                    "{{\"timestamp\":\"{}\",\"ordinal\":1,\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_started\",\"turn_id\":\"turn-synthetic\"}}}}\n",
                    "{{\"timestamp\":\"{}\",\"ordinal\":2,\"type\":\"event_msg\",\"payload\":{{\"type\":\"user_message\",\"turn_id\":\"turn-synthetic\",\"message\":\"semantic user body\"}}}}\n",
                    "{{\"timestamp\":\"{}\",\"ordinal\":3,\"type\":\"response_item\",\"payload\":{{\"type\":\"message\",\"role\":\"user\",\"content\":[{{\"type\":\"input_text\",\"text\":\"semantic user body\"}}]}}}}\n",
                    "{{\"timestamp\":\"{}\",\"ordinal\":4,\"type\":\"response_item\",\"payload\":{{\"type\":\"function_call_output\",\"call_id\":\"call_synthetic\",\"output\":\"semantic tool body\"}}}}\n",
                    "{{\"timestamp\":\"{}\",\"ordinal\":5,\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_complete\",\"turn_id\":\"turn-synthetic\"}}}}\n"
                ),
                stamp, logical, logical, stamp, stamp, stamp, stamp, stamp, stamp
            ),
        )
        .unwrap();
        let bootstrap = std::process::Command::new(&cli)
            .env("CODEX_HOME", &home)
            .env_remove("CODEX_SQLITE_HOME")
            .env("OTEL_SDK_DISABLED", "true")
            .arg("migrate-rollouts")
            .arg("--apply")
            .arg("--json")
            .arg("--thread")
            .arg(logical)
            .output()
            .unwrap();
        assert!(
            bootstrap.status.success(),
            "bootstrap failed: {}",
            String::from_utf8_lossy(&bootstrap.stderr)
        );

        let manifest = temp.path().join("manifest.json");
        // 这一轮必须直接覆盖 legacy relay（24 bytes）-> openai（6 bytes）；
        // custom -> openai 是等长切换，不能替代该非等长 CLI 验证。
        let (result, openai_paths) =
            repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
                &home,
                "openai",
                Some(&manifest),
                Some(cli.to_str().unwrap()),
                || Ok(true),
            )
            .unwrap();
        assert!(result.repair_complete);
        assert!(
            openai_paths
                .iter()
                .any(|path| path.ends_with(THREAD_HISTORY_FILE))
        );
        let final_rollout = fs::read_to_string(&rollout).unwrap();
        assert!(final_rollout.contains("\"model_provider\":\"openai\""));
        assert!(final_rollout.contains("semantic user body"));
        assert!(final_rollout.contains("semantic tool body"));
        let (openai_repeat, openai_repeat_paths) =
            repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
                &home,
                "openai",
                Some(&manifest),
                Some(cli.to_str().unwrap()),
                || Ok(true),
            )
            .unwrap();
        assert_eq!(openai_repeat.files_modified, 0);
        assert!(openai_repeat_paths.is_empty());

        let (to_custom, custom_paths) =
            repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
                &home,
                "custom",
                Some(&manifest),
                Some(cli.to_str().unwrap()),
                || Ok(true),
            )
            .unwrap();
        assert!(to_custom.repair_complete);
        assert_eq!(to_custom.files_modified, 1);
        assert!(
            !custom_paths.iter().any(|path| {
                path.file_name()
                    .is_some_and(|name| name == THREAD_HISTORY_FILE)
            }),
            "健康等长切换不得调用历史迁移器：{openai_paths:#?}"
        );
        let (custom_repeat, custom_repeat_paths) =
            repair_after_connection_switch_preserving_history_with_guard_at_with_paths_for_app(
                &home,
                "custom",
                Some(&manifest),
                Some(cli.to_str().unwrap()),
                || Ok(true),
            )
            .unwrap();
        assert_eq!(custom_repeat.files_modified, 0);
        assert!(custom_repeat_paths.is_empty());
        let final_rollout = fs::read_to_string(&rollout).unwrap();
        assert!(final_rollout.contains("\"model_provider\":\"custom\""));
        assert!(final_rollout.contains("semantic user body"));
        assert!(final_rollout.contains("semantic tool body"));
        let state = Connection::open(home.join("state_5.sqlite")).unwrap();
        let mode: String = state
            .query_row(
                "SELECT history_mode FROM threads WHERE id=?1",
                [logical],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mode, "paginated");
    }

    #[test]
    fn migration_report_requires_one_exact_physical_outcome() {
        let temp = tempfile::tempdir().unwrap();
        let logical = "00000000-0000-7000-8000-00000000000a";
        let physical = "00000000-0000-7000-8000-00000000000b";
        let path = temp.path().join(format!(
            "rollout-2026-09-05T12-00-00-{logical}_{physical}.jsonl"
        ));
        fs::write(&path, b"synthetic").unwrap();
        let plan = single_recovery_plan(logical, physical, path.clone(), false);
        let report = MigrationReport {
            outcomes: vec![MigrationOutcome {
                thread_id: Some(logical.into()),
                rollout_path: path,
                status: "migrated".into(),
                message: None,
            }],
        };
        validate_migration_report(std::slice::from_ref(&plan), &report).unwrap();
        let duplicate = MigrationReport {
            outcomes: vec![
                MigrationOutcome {
                    thread_id: Some(logical.into()),
                    rollout_path: report.outcomes[0].rollout_path.clone(),
                    status: "migrated".into(),
                    message: None,
                },
                MigrationOutcome {
                    thread_id: Some(logical.into()),
                    rollout_path: report.outcomes[0].rollout_path.clone(),
                    status: "migrated".into(),
                    message: None,
                },
            ],
        };
        assert!(validate_migration_report(std::slice::from_ref(&plan), &duplicate).is_err());
    }
    #[test]
    fn sqlite_backup_uses_consistent_wal_snapshot_and_restore_attempts_all_targets() {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("state_5.sqlite");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE entries(value TEXT);")
            .unwrap();
        connection
            .execute("INSERT INTO entries VALUES('before')", [])
            .unwrap();
        let mut backup = RepairBackup::create().unwrap();
        backup.add_sqlite_database(&database).unwrap();
        connection
            .execute("INSERT INTO entries VALUES('after')", [])
            .unwrap();
        drop(connection);
        backup.restore().unwrap();
        let restored = Connection::open_with_flags(
            &database,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        let values = restored
            .prepare("SELECT value FROM entries ORDER BY rowid")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(values, ["before"]);
        let integrity: String = restored
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        drop(restored);
        backup.cleanup().unwrap();

        let good = temp.path().join("good.jsonl");
        let bad = temp.path().join("bad.jsonl");
        fs::write(&good, b"good-before").unwrap();
        fs::write(&bad, b"bad-before").unwrap();
        let mut backup = RepairBackup::create().unwrap();
        backup.add_bytes(&good, b"good-before").unwrap();
        backup.add_bytes(&bad, b"bad-before").unwrap();
        fs::write(&good, b"good-after").unwrap();
        fs::write(&bad, b"bad-after").unwrap();
        fs::remove_file(&backup.entries[1].backup).unwrap();
        let manifest: Value =
            serde_json::from_slice(&fs::read(backup.dir.join("backup-manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest.as_array().unwrap().len(), 2);
        let error = backup.restore().unwrap_err();
        assert!(error.to_string().contains("bad.jsonl"));
        assert_eq!(fs::read(&good).unwrap(), b"good-before");
        assert_eq!(fs::read(&bad).unwrap(), b"bad-after");
        assert!(backup.dir.exists());
        backup.cleanup().unwrap();
    }

    #[test]
    fn corrupt_backup_is_detected_before_its_target_is_touched() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target.jsonl");
        fs::write(&target, b"before").unwrap();
        let mut backup = RepairBackup::create().unwrap();
        backup.add_bytes(&target, b"before").unwrap();
        fs::write(&target, b"external-after").unwrap();
        fs::write(&backup.entries[0].backup, b"corrupt").unwrap();

        let error = backup.restore_entry(&backup.entries[0]).unwrap_err();

        assert!(error.to_string().contains("哈希核验失败"));
        assert_eq!(fs::read(&target).unwrap(), b"external-after");
        let manifest: Value =
            serde_json::from_slice(&fs::read(backup.dir.join("backup-manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest[0]["type"], "bytes");
        backup.cleanup().unwrap();
    }

    #[test]
    fn corrupt_sqlite_snapshot_is_detected_before_wal_or_target_is_touched() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("state_5.sqlite");
        let connection = Connection::open(&target).unwrap();
        connection
            .execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE entries(value TEXT);")
            .unwrap();
        connection
            .execute("INSERT INTO entries VALUES('before')", [])
            .unwrap();
        let mut backup = RepairBackup::create().unwrap();
        backup.add_sqlite_database(&target).unwrap();
        connection
            .execute("INSERT INTO entries VALUES('external-after')", [])
            .unwrap();
        drop(connection);
        let target_before = fs::read(&target).unwrap();
        let wal_path = PathBuf::from(format!("{}-wal", target.display()));
        let wal_before = wal_path.is_file().then(|| fs::read(&wal_path).unwrap());
        fs::write(&backup.entries[0].backup, b"corrupt").unwrap();

        let error = backup.restore_entry(&backup.entries[0]).unwrap_err();

        assert!(error.to_string().contains("SQLite 备份快照哈希核验失败"));
        assert_eq!(fs::read(&target).unwrap(), target_before);
        assert_eq!(
            wal_path.is_file().then(|| fs::read(&wal_path).unwrap()),
            wal_before
        );
        backup.cleanup().unwrap();
    }

    #[test]
    fn post_rename_write_error_is_registered_and_rolled_back() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("rollout.jsonl");
        fs::write(&target, b"before").unwrap();
        let mut backup = RepairBackup::create().unwrap();
        backup.add_bytes(&target, b"before").unwrap();
        let mut written = HashSet::new();

        let error = track_atomic_write_result(&target, b"after", &mut written, || {
            atomic_write(&target, b"after")?;
            anyhow::bail!("模拟 rename 后目录 fsync 失败")
        })
        .unwrap_err();

        assert!(error.to_string().contains("rename 后目录 fsync"));
        assert!(written.contains(&target));
        backup.restore_selected(&written).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"before");
        backup.cleanup().unwrap();
    }

    #[test]
    fn database_rows_zero_after_preflight_is_a_concurrent_conflict() {
        let temp = tempfile::tempdir().unwrap();
        let database_path = temp.path().join("state_5.sqlite");
        let database = Connection::open(&database_path).unwrap();
        database
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT);
                 INSERT INTO threads VALUES('one','openai');",
            )
            .unwrap();
        let scope = session_scope(std::slice::from_ref(&database_path), &[]).unwrap();
        assert_eq!(
            preflight_database(&database_path, "custom", &scope).unwrap(),
            1
        );
        database
            .execute(
                "UPDATE threads SET model_provider='custom' WHERE id='one'",
                [],
            )
            .unwrap();
        drop(database);

        let error = repair_database_commit(&database_path, "custom", &scope, 1, &mut || Ok(true))
            .unwrap_err();

        assert!(error.to_string().contains("并发变化"));
        assert_sqlite_integrity(&database_path);
    }

    #[test]
    fn migration_command_deduplicates_logical_thread_requests() {
        let temp = tempfile::tempdir().unwrap();
        let logical = "00000000-0000-7000-8000-0000000000b2";
        let first = single_recovery_plan(
            logical,
            logical,
            temp.path()
                .join(format!("rollout-2026-09-05T12-00-00-{logical}.jsonl")),
            false,
        );
        let second = single_recovery_plan(
            logical,
            logical,
            temp.path()
                .join(format!("rollout-2026-09-05T12-01-00-{logical}.jsonl")),
            false,
        );
        // 跨平台构建 Codex 应用/CLI 夹具：Windows 用 .exe + resources CLI，
        // macOS 用 .app bundle，其余 Unix 直接用可执行文件；Unix 需显式设置
        // 可执行位，否则权限校验会拒绝普通文件。
        let configured_app = {
            #[cfg(windows)]
            {
                let app = temp.path().join("Custom/Codex.exe");
                let cli = temp.path().join("Custom/resources/codex.exe");
                fs::create_dir_all(cli.parent().unwrap()).unwrap();
                fs::write(&app, b"desktop").unwrap();
                fs::write(&cli, b"cli").unwrap();
                app
            }
            #[cfg(target_os = "macos")]
            {
                let bundle = temp.path().join("Custom.app");
                let cli = bundle.join("Contents/Resources/codex");
                fs::create_dir_all(bundle.join("Contents/MacOS")).unwrap();
                fs::create_dir_all(cli.parent().unwrap()).unwrap();
                fs::write(&cli, b"cli").unwrap();
                make_executable(&cli);
                bundle
            }
            #[cfg(all(unix, not(target_os = "macos")))]
            {
                let app = temp.path().join("Custom/codex");
                fs::create_dir_all(app.parent().unwrap()).unwrap();
                fs::write(&app, b"cli").unwrap();
                make_executable(&app);
                app
            }
        };
        let command = codex_history_migration_command(
            temp.path(),
            Some(configured_app.to_string_lossy().as_ref()),
            &[first, second],
        )
        .unwrap();
        let args = command
            .get_args()
            .filter_map(|arg| arg.to_str())
            .collect::<Vec<_>>();
        assert_eq!(
            args.windows(2)
                .filter(|pair| pair[0] == "--thread" && pair[1] == logical)
                .count(),
            1
        );
    }
    #[test]
    fn preflight_uses_physical_rollout_id_for_healthy_abc_lineage() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex");
        let sessions = home.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let logical = "00000000-0000-7000-8000-00000000000a";
        let physical_a = logical;
        let physical_b = "00000000-0000-7000-8000-00000000000b";
        let physical_c = "00000000-0000-7000-8000-00000000000c";
        let path_a = sessions.join(format!("rollout-2026-09-05T12-00-00-{logical}.jsonl"));
        let path_b = sessions.join(format!(
            "rollout-2026-09-05T12-01-00-{logical}_{physical_b}.jsonl"
        ));
        let path_c = sessions.join(format!(
            "rollout-2026-09-05T12-02-00-{logical}_{physical_c}.jsonl"
        ));
        let write_rollout = |path: &Path,
                             ordinal: u64,
                             history_base: Option<Value>,
                             filler_length: usize| {
            fs::write(
                path,
                format!(
                    "{}\n{}\n",
                    serde_json::json!({
                        "ordinal": ordinal,
                        "type": "session_meta",
                        "payload": {
                            "id": logical,
                            "history_mode": "paginated",
                            "model_provider": "openai",
                            "history_base": history_base
                        }
                    }),
                    serde_json::json!({
                        "ordinal": ordinal + 1,
                        "type": "event_msg",
                        "payload": {
                            "type": "task_started",
                            "turn_id": format!("turn-{}", path.file_stem().unwrap().to_string_lossy()),
                            "filler": "x".repeat(filler_length)
                        }
                    })
                ),
            )
            .unwrap()
        };
        write_rollout(&path_a, 0, None, 600);
        let length_a = fs::metadata(&path_a).unwrap().len();
        write_rollout(
            &path_b,
            2,
            Some(serde_json::json!({
                "thread_id": physical_a,
                "end_ordinal_exclusive": 2,
                "end_byte_offset": length_a
            })),
            60,
        );
        let length_b = fs::metadata(&path_b).unwrap().len();
        write_rollout(
            &path_c,
            4,
            Some(serde_json::json!({
                "thread_id": physical_b,
                "end_ordinal_exclusive": 4,
                "end_byte_offset": length_b
            })),
            160,
        );
        let paths = [
            (&path_a, physical_a, 0_i64),
            (&path_b, physical_b, 2_i64),
            (&path_c, physical_c, 4_i64),
        ];
        let state_path = home.join("state_5.sqlite");
        let state = Connection::open(&state_path).unwrap();
        state
            .execute_batch(&format!(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, history_mode TEXT, archived INTEGER, source TEXT);
                 INSERT INTO threads VALUES('{logical}','paginated',0,NULL);"
            ))
            .unwrap();
        drop(state);

        let history = Connection::open(home.join(THREAD_HISTORY_FILE)).unwrap();
        history
            .execute_batch(
                "CREATE TABLE thread_history_projection_state(
                     thread_id TEXT PRIMARY KEY,
                     next_rollout_byte_offset INTEGER,
                     next_rollout_ordinal INTEGER
                 );
                 CREATE TABLE thread_items(thread_id TEXT, rollout_ordinal INTEGER, item_json TEXT);
                 CREATE TABLE thread_turns(
                     thread_id TEXT,
                     rollout_ordinal INTEGER,
                     rollout_byte_offset INTEGER,
                     rollout_end_ordinal INTEGER,
                     rollout_end_byte_offset INTEGER
                 );",
            )
            .unwrap();
        for (path, physical, start_ordinal) in &paths {
            let length = i64::try_from(fs::metadata(path).unwrap().len()).unwrap();
            history
                .execute(
                    "INSERT INTO thread_history_projection_state VALUES(?1,?2,?3)",
                    (physical, length, start_ordinal + 2),
                )
                .unwrap();
            history
                .execute(
                    "INSERT INTO thread_items VALUES(?1,?2,'{}')",
                    (*physical, start_ordinal + 1),
                )
                .unwrap();
            history
                .execute(
                    "INSERT INTO thread_turns VALUES(?1,?2,0,?3,?4)",
                    (*physical, start_ordinal, start_ordinal + 1, length),
                )
                .unwrap();
        }
        drop(history);

        let rollouts = rollout_files(&home);
        let scope = session_scope(std::slice::from_ref(&state_path), &rollouts).unwrap();
        let plans = preflight_paginated_history_recovery(&home, &scope, &rollouts, &HashSet::new())
            .unwrap();

        assert!(
            plans.is_empty(),
            "healthy A/B/C lineage must not be recovered: {plans:#?}"
        );

        let history = Connection::open(home.join(THREAD_HISTORY_FILE)).unwrap();
        history
            .execute(
                "DELETE FROM thread_history_projection_state WHERE thread_id=?1",
                [physical_b],
            )
            .unwrap();
        drop(history);
        let error = preflight_paginated_history_recovery(
            &home,
            &scope,
            &rollouts,
            &HashSet::from([path_a.clone()]),
        )
        .unwrap_err();
        let error = error.to_string();
        assert!(error.contains("写入前拒绝恢复"));
        assert!(error.contains(path_a.to_string_lossy().as_ref()));
        assert!(error.contains(path_b.to_string_lossy().as_ref()));
        assert!(error.contains(path_c.to_string_lossy().as_ref()));
    }

    #[test]
    fn rollout_filename_parser_distinguishes_logical_and_physical_ids() {
        let logical = "00000000-0000-7000-8000-00000000000a";
        let physical = "00000000-0000-7000-8000-00000000000b";
        let single = PathBuf::from(format!("rollout-2026-09-05T12-00-00-{logical}.jsonl"));
        assert_eq!(
            rollout_ids_from_path(&single).unwrap(),
            (logical.into(), logical.into())
        );
        let continued = PathBuf::from(format!(
            "rollout-2026-09-05T12-00-01-{logical}_{physical}.jsonl"
        ));
        assert_eq!(
            rollout_ids_from_path(&continued).unwrap(),
            (logical.into(), physical.into())
        );
        assert!(
            rollout_ids_from_path(Path::new(
                "rollout-2026-99-05T12-00-00-00000000-0000-7000-8000-00000000000a.jsonl"
            ))
            .is_err()
        );
    }
}
