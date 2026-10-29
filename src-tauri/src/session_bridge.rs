//! One CDP connection to the already running Codex desktop. Native notifications
//! update our catalog; no direct writes to Codex databases or rollout files.
use crate::{
    codex, desktop_cdp,
    models::{AppError, SessionAffectedThread, SessionRuntime, SessionSummary, SessionSyncStatus},
    session_index::SessionIndex,
    storage::Store,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::RwLock,
    time::Duration,
};
use tauri::{Emitter, Manager};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

const EVENT: &str = "sessions-changed";
const BINDING: &str = "__codexToolsSessionEvent";
const DISCONNECTED: &str = "请完全退出 Codex，再从 CodexTools 的概览页启动 Codex以开启会话同步。";
const SCRIPT: &str = include_str!("session_bridge.js");

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConnectionSettings {
    home: PathBuf,
    last_debug_port: Option<u16>,
}

impl ConnectionSettings {
    fn read(store: &Store) -> Result<Self, AppError> {
        store.read(|state| Self {
            home: codex::home(&state.codex.home),
            last_debug_port: state.codex.last_debug_port,
        })
    }

    fn ensure_current(&self, store: &Store) -> Result<(), AppError> {
        if Self::read(store)? != *self {
            return Err(AppError::InvalidConfig(
                "Codex 会话目录或调试端口已变化，正在重新连接。".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct NativeThread {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default, rename = "fallbackTitle")]
    fallback_title: String,
    #[serde(default)]
    preview: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    status: Value,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    provider: String,
    #[serde(default, rename = "updatedAt")]
    updated_at: i64,
}

impl NativeThread {
    fn is_list_visible(&self) -> bool {
        self.archived
            || self
                .preview
                .as_ref()
                .is_none_or(|preview| !preview.is_empty())
    }

    fn runtime(&self) -> SessionRuntime {
        let state = match self.status.get("type").and_then(Value::as_str) {
            Some("active") => "running",
            Some("idle" | "notLoaded") => "idle",
            _ => "unknown",
        };
        SessionRuntime {
            thread_id: self.id.clone(),
            state: state.into(),
            blocked_reason: match state {
                "running" => Some("会话正在运行，请等待完成后再操作。".into()),
                "unknown" => Some("无法确认会话运行状态，暂不能操作。".into()),
                _ => None,
            },
        }
    }
}

#[derive(Default)]
struct Snapshot {
    connected: bool,
    home: PathBuf,
    threads: HashMap<String, NativeThread>,
    revision: u64,
    generation: u64,
    events: VecDeque<(u64, Value)>,
    deleted: HashSet<String>,
    confirmed: HashMap<String, String>,
}

#[derive(Deserialize)]
struct NativeScope {
    threads: Vec<NativeThread>,
    #[serde(default, rename = "blockedReason")]
    blocked_reason: Option<String>,
}

struct Evaluation {
    expression: String,
    result: oneshot::Sender<Result<Value, AppError>>,
}

#[derive(Default)]
pub(crate) struct SessionBridge {
    snapshot: RwLock<Snapshot>,
    sender: RwLock<Option<mpsc::Sender<Evaluation>>>,
    pub operation: Mutex<()>,
    /// A timed-out mutation must be reconciled before another write is allowed.
    pub uncertain: Mutex<HashMap<String, (String, Vec<String>)>>,
}

impl SessionBridge {
    pub async fn refresh_catalog(&self, app: &tauri::AppHandle) -> Result<(), AppError> {
        let (generation, revision) = {
            let snapshot = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
            (snapshot.generation, snapshot.revision)
        };
        let threads: Vec<NativeThread> = serde_json::from_value(
            self.evaluate("window.__codexToolsSessionsV1.list()".into())
                .await?,
        )
        .map_err(|_| AppError::Internal("Codex 会话目录响应无效。".into()))?;
        {
            let mut snapshot = self.snapshot.write().unwrap_or_else(|p| p.into_inner());
            replace_catalog(&mut snapshot, generation, revision, threads)?;
        }
        self.publish(app, true);
        Ok(())
    }

    pub fn applied(&self, action: &str, id: &str, affected: &[String]) -> bool {
        let snapshot = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
        if !snapshot.connected {
            return false;
        }
        match action {
            "delete" => {
                !affected.is_empty()
                    && affected.iter().all(|id| {
                        snapshot.deleted.contains(id)
                            || snapshot
                                .confirmed
                                .get(id)
                                .is_some_and(|action| action == "delete")
                    })
            }
            "archive" => snapshot.threads.get(id).is_some_and(|t| t.archived),
            "unarchive" => snapshot.threads.get(id).is_some_and(|t| !t.archived),
            _ => false,
        }
    }

    /// A native successful response proves the mutation even when the native list
    /// intentionally hides empty active threads. Never infer deletion from absence.
    pub fn record_success(
        &self,
        action: &str,
        id: &str,
        affected: &[String],
        home: &std::path::Path,
        outcome: &Value,
    ) -> bool {
        if !outcome_confirms(action, id, home, outcome) {
            return false;
        }
        let mut snapshot = self.snapshot.write().unwrap_or_else(|p| p.into_inner());
        if !snapshot.connected || snapshot.home != home {
            return false;
        }
        if action == "delete" {
            for id in affected {
                snapshot.confirmed.insert(id.clone(), action.to_owned());
            }
        }
        true
    }

    pub async fn confirm_applied(
        &self,
        action: &str,
        id: &str,
        affected: &[String],
        home: &std::path::Path,
    ) -> Result<bool, AppError> {
        let generation = {
            let snapshot = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
            if !snapshot.connected || snapshot.home != home {
                return Ok(false);
            }
            snapshot.generation
        };
        if action == "delete" {
            // The native thread/read "thread not loaded" error also covers live
            // persisted threads, so only explicit deletion evidence is conclusive.
            return Ok(self.applied(action, id, affected));
        }
        if !matches!(action, "archive" | "unarchive") {
            return Ok(false);
        }
        let response = self
            .rpc("thread/read", json!({"threadId":id,"includeTurns":false}))
            .await?;
        let snapshot = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
        if !snapshot.connected || snapshot.home != home || snapshot.generation != generation {
            return Err(AppError::Internal("Codex 连接已变化，请重新核对。".into()));
        }
        let confirmed = response
            .get("thread")
            .is_some_and(|thread| native_thread_in_location(thread, id, home, action == "archive"));
        Ok(confirmed)
    }
    pub fn status(&self, ids: &[String], home: &std::path::Path) -> SessionSyncStatus {
        let snapshot = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
        let connected = snapshot.connected && snapshot.home == home;
        SessionSyncStatus {
            connected,
            message: if connected {
                "已连接 Codex，会话操作将自动同步。".into()
            } else {
                DISCONNECTED.into()
            },
            threads: ids
                .iter()
                .map(|id| {
                    let mut state = snapshot
                        .threads
                        .get(id)
                        .map(NativeThread::runtime)
                        .unwrap_or(SessionRuntime {
                            thread_id: id.clone(),
                            state: "unknown".into(),
                            blocked_reason: Some("此会话未在当前 Codex 会话服务中找到。".into()),
                        });
                    if !connected {
                        state.state = "unknown".into();
                        state.blocked_reason = Some(DISCONNECTED.into());
                    }
                    state
                })
                .collect(),
        }
    }

    pub fn thread(&self, id: &str, home: &std::path::Path) -> Result<NativeThread, AppError> {
        let snapshot = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
        if !snapshot.connected || snapshot.home != home {
            return Err(AppError::InvalidConfig(DISCONNECTED.into()));
        }
        let thread = snapshot.threads.get(id).cloned().ok_or_else(|| {
            AppError::InvalidConfig("当前 Codex 中没有这条会话，请刷新列表。".into())
        })?;
        if let Some(path) = thread.path.as_deref() {
            // Do not mutate a desktop using a different CODEX_HOME, even if cloned
            // files in both homes happen to carry the same logical thread UUID.
            let native = PathBuf::from(path);
            if !native.starts_with(home) {
                return Err(AppError::InvalidConfig(
                    "Codex 与 CodexTools 使用的会话目录不同，请先统一设置。".into(),
                ));
            }
        }
        Ok(thread)
    }

    pub async fn evaluate(&self, expression: String) -> Result<Value, AppError> {
        let sender = self
            .sender
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(|| AppError::InvalidConfig(DISCONNECTED.into()))?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(Evaluation {
                expression,
                result: tx,
            })
            .await
            .map_err(|_| {
                AppError::Internal("Codex 同步连接已中断；请重新连接并核对会话状态。".into())
            })?;
        tokio::time::timeout(Duration::from_secs(30), rx)
            .await
            .map_err(|_| {
                AppError::Internal("等待 Codex 响应超时；请核对会话状态后再操作。".into())
            })?
            .map_err(|_| {
                AppError::Internal("Codex 同步连接已中断；请核对操作是否已生效。".into())
            })?
    }

    pub async fn rpc(&self, method: &str, params: Value) -> Result<Value, AppError> {
        if !matches!(method, "thread/read" | "thread/list" | "thread/loaded/list") {
            return Err(AppError::InvalidConfig("不支持的会话查询。".into()));
        }
        self.evaluate(format!(
            "window.__codexToolsSessionsV1.request({}, {})",
            json!(method),
            params
        ))
        .await
    }

    pub async fn affected(
        &self,
        id: &str,
        home: &std::path::Path,
    ) -> Result<(Vec<SessionAffectedThread>, Option<String>), AppError> {
        self.thread(id, home)?;
        let scope: NativeScope = serde_json::from_value(
            self.evaluate(format!(
                "window.__codexToolsSessionsV1.affectedScope({})",
                json!(id)
            ))
            .await?,
        )
        .map_err(|_| AppError::Internal("Codex 派生会话响应无效。".into()))?;
        let threads = scope
            .threads
            .into_iter()
            .map(|thread| SessionAffectedThread {
                id: thread.id,
                title: thread.title,
            })
            .collect();
        Ok((threads, scope.blocked_reason))
    }

    pub async fn mutate(
        &self,
        method: &str,
        id: &str,
        affected: &[String],
    ) -> Result<Value, AppError> {
        if !matches!(
            method,
            "thread/archive" | "thread/unarchive" | "thread/delete"
        ) {
            return Err(AppError::InvalidConfig("不支持的会话操作。".into()));
        }
        self.evaluate(format!(
            "window.__codexToolsSessionsV1.guardAndMutate({}, {}, {})",
            json!(method),
            json!(id),
            json!(affected)
        ))
        .await
    }

    fn apply_notification(&self, notification: &Value) -> bool {
        let method = notification
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut snapshot = self.snapshot.write().unwrap_or_else(|p| p.into_inner());
        snapshot.revision = snapshot.revision.saturating_add(1);
        let revision = snapshot.revision;
        snapshot.events.push_back((revision, notification.clone()));
        if snapshot.events.len() > 1024 {
            snapshot.events.pop_front();
        }
        if let Some(id) = notification.get("threadId").and_then(Value::as_str) {
            if method == "thread/deleted" {
                snapshot.deleted.insert(id.to_owned());
                snapshot.confirmed.remove(id);
            } else if matches!(method, "thread/archived" | "thread/unarchived") {
                snapshot.deleted.remove(id);
                snapshot.confirmed.remove(id);
            }
        } else if method == "thread/started" {
            if let Some(id) = notification.pointer("/thread/id").and_then(Value::as_str) {
                snapshot.deleted.remove(id);
                snapshot.confirmed.remove(id);
            }
        }
        update_threads(&mut snapshot.threads, notification);
        matches!(
            method,
            "thread/started"
                | "thread/archived"
                | "thread/unarchived"
                | "thread/deleted"
                | "thread/name/updated"
        )
    }

    fn needs_catalog_refresh(&self, notification: &Value) -> bool {
        let Some(id) = notification.get("threadId").and_then(Value::as_str) else {
            return false;
        };
        let snapshot = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
        let thread = snapshot.threads.get(id);
        if thread.is_some_and(|thread| thread.is_list_visible() && thread.path.is_some()) {
            return false;
        }
        match notification.get("method").and_then(Value::as_str) {
            Some("thread/status/changed") => {
                thread.and_then(|thread| thread.status.get("type"))
                    != notification.pointer("/status/type")
            }
            Some(
                "turn/started"
                | "turn/completed"
                | "thread/name/updated"
                | "thread/archived"
                | "thread/unarchived",
            ) => true,
            _ => false,
        }
    }

    fn publish(&self, app: &tauri::AppHandle, catalog_changed: bool) {
        if catalog_changed {
            let snapshot = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
            if snapshot.connected {
                app.state::<SessionIndex>().set_live_catalog(
                    snapshot.home.clone(),
                    snapshot
                        .threads
                        .iter()
                        .filter(|(_, t)| t.is_list_visible())
                        .map(|(id, t)| (id.clone(), t.archived))
                        .collect(),
                    snapshot
                        .threads
                        .values()
                        .filter(|t| {
                            t.parent.is_none()
                                && t.is_list_visible()
                                && t.path
                                    .as_ref()
                                    .is_some_and(|p| PathBuf::from(p).starts_with(&snapshot.home))
                        })
                        .map(|t| SessionSummary {
                            identity: format!("native#{}", t.id),
                            id: t.id.clone(),
                            title: t.title.clone(),
                            provider: t.provider.clone(),
                            cwd: t.cwd.clone(),
                            archived: t.archived,
                            updated_at: if t.updated_at < 1_000_000_000_000 {
                                t.updated_at.saturating_mul(1000)
                            } else {
                                t.updated_at
                            },
                            source_db: String::new(),
                            source_rollout: t.path.clone(),
                            original_provider: t.provider.clone(),
                            has_user_event: true,
                        })
                        .collect(),
                );
            }
        }
        let kind = if catalog_changed {
            "catalog"
        } else if self
            .snapshot
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .connected
        {
            "runtime"
        } else {
            "connection"
        };
        let _ = app.emit(EVENT, json!({"kind":kind}));
    }
}

fn native_thread_in_location(
    thread: &Value,
    id: &str,
    home: &std::path::Path,
    archived: bool,
) -> bool {
    let Some(path) = thread.get("path").and_then(Value::as_str) else {
        return false;
    };
    let path = PathBuf::from(path);
    thread.get("id").and_then(Value::as_str) == Some(id)
        && !path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
        && path.starts_with(home.join(if archived {
            "archived_sessions"
        } else {
            "sessions"
        }))
}

fn outcome_confirms(action: &str, id: &str, home: &std::path::Path, outcome: &Value) -> bool {
    if outcome.get("dispatched").and_then(Value::as_bool) != Some(true)
        || outcome.get("error").is_some()
    {
        return false;
    }
    match action {
        "unarchive" => outcome
            .pointer("/value/thread")
            .is_some_and(|thread| native_thread_in_location(thread, id, home, false)),
        "archive" | "delete" => outcome
            .get("value")
            .and_then(Value::as_object)
            .is_some_and(|value| value.is_empty()),
        _ => false,
    }
}

fn update_threads(threads: &mut HashMap<String, NativeThread>, notification: &Value) {
    let method = notification
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if method == "thread/started" {
        if let Some(thread) = notification
            .get("thread")
            .and_then(|v| serde_json::from_value::<NativeThread>(v.clone()).ok())
        {
            let mut thread = thread;
            if thread.preview.is_none() {
                thread.preview = Some(String::new());
            }
            threads.insert(thread.id.clone(), thread);
        }
    } else if let Some(id) = notification.get("threadId").and_then(Value::as_str) {
        if method == "thread/deleted" {
            threads.remove(id);
        } else {
            if method == "thread/status/changed" {
                threads
                    .entry(id.to_owned())
                    .or_insert_with(|| NativeThread {
                        id: id.to_owned(),
                        preview: Some(String::new()),
                        ..NativeThread::default()
                    });
            }
            if let Some(thread) = threads.get_mut(id) {
                match method {
                    "thread/archived" => thread.archived = true,
                    "thread/unarchived" => thread.archived = false,
                    "thread/name/updated" => match notification.get("name") {
                        Some(Value::String(name)) if !name.trim().is_empty() => {
                            thread.title.clone_from(name);
                        }
                        Some(Value::String(_)) | Some(Value::Null) => {
                            thread.title = if thread.fallback_title.trim().is_empty() {
                                "未命名会话".into()
                            } else {
                                thread.fallback_title.clone()
                            };
                        }
                        _ => {}
                    },
                    "thread/status/changed" => {
                        thread.status = notification.get("status").cloned().unwrap_or(Value::Null)
                    }
                    _ => {}
                }
            }
        }
    }
}

fn replace_catalog(
    snapshot: &mut Snapshot,
    generation: u64,
    revision: u64,
    threads: Vec<NativeThread>,
) -> Result<(), AppError> {
    if !snapshot.connected || snapshot.generation != generation {
        return Err(AppError::Internal("Codex 连接已变化，请重新核对。".into()));
    }
    if snapshot
        .events
        .front()
        .is_some_and(|(first, _)| *first > revision.saturating_add(1))
    {
        return Err(AppError::Internal(
            "核对期间会话变化过多，请重新核对状态。".into(),
        ));
    }
    let hidden: Vec<_> = snapshot
        .threads
        .values()
        .filter(|thread| !thread.is_list_visible() && !snapshot.deleted.contains(&thread.id))
        .cloned()
        .collect();
    snapshot.threads = threads
        .into_iter()
        .filter(|thread| !snapshot.deleted.contains(&thread.id))
        .map(|t| (t.id.clone(), t))
        .collect();
    for thread in hidden {
        snapshot.threads.entry(thread.id.clone()).or_insert(thread);
    }
    for (_, event) in snapshot.events.iter().filter(|(n, _)| *n > revision) {
        update_threads(&mut snapshot.threads, event);
    }
    Ok(())
}

#[cfg(test)]
fn subtree(id: &str, threads: &[NativeThread]) -> Vec<NativeThread> {
    let mut selected = HashSet::from([id.to_string()]);
    loop {
        let before = selected.len();
        for thread in threads {
            if thread.parent.as_ref().is_some_and(|p| selected.contains(p)) {
                selected.insert(thread.id.clone());
            }
        }
        if before == selected.len() {
            break;
        }
    }
    let mut result: Vec<_> = threads
        .iter()
        .filter(|thread| thread.id != id && selected.contains(&thread.id))
        .cloned()
        .collect();
    result.sort_by(|a, b| a.id.cmp(&b.id));
    result.dedup_by(|a, b| a.id == b.id);
    result
}

pub(crate) async fn run(app: tauri::AppHandle) {
    loop {
        let bridge = app.state::<SessionBridge>();
        let store = app.state::<Store>();
        let settings = match ConnectionSettings::read(&store) {
            Ok(settings) => settings,
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        if let Some((_, url)) = desktop_cdp::find_codex_debug_page_on(
            &desktop_cdp::debug_probe_ports_for(settings.last_debug_port),
        )
        .await
        {
            let _ = run_connection(&app, &bridge, settings, &url).await;
        }
        *bridge.sender.write().unwrap_or_else(|p| p.into_inner()) = None;
        let changed = {
            let mut snapshot = bridge.snapshot.write().unwrap_or_else(|p| p.into_inner());
            let changed = snapshot.connected;
            snapshot.connected = false;
            changed
        };
        if changed {
            bridge.publish(&app, false);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn run_connection(
    app: &tauri::AppHandle,
    bridge: &SessionBridge,
    settings: ConnectionSettings,
    url: &str,
) -> Result<(), AppError> {
    let store = app.state::<Store>();
    settings.ensure_current(&store)?;
    if !desktop_cdp::validate_debug_ws_url(url, None) {
        return Err(AppError::InvalidConfig("无效的 Codex 调试地址。".into()));
    }
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .map_err(|_| AppError::Internal("连接 Codex 超时。".into()))?
    .map_err(|_| AppError::Internal("无法连接 Codex。".into()))?;
    // Setup and catalog loading precede enabling buttons. Buffer notifications
    // during pagination, then replay them so concurrent changes cannot resurrect rows.
    let mut notifications = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(8),
        initialize_connection(&mut socket, &mut notifications),
    )
    .await
    .map_err(|_| AppError::Internal("初始化 Codex 连接超时。".into()))??;
    socket
        .send(Message::Text(
            evaluation_request(3, "window.__codexToolsSessionsV1.list()").to_string(),
        ))
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let wait_catalog = async {
        let response = read_cdp_response(&mut socket, 3, &mut notifications).await?;
        desktop_cdp::parse_evaluation_response(&response)
    };
    let catalog = tokio::time::timeout(Duration::from_secs(60), wait_catalog)
        .await
        .map_err(|_| AppError::Internal("读取 Codex 会话目录超时。".into()))??;
    let threads: Vec<NativeThread> =
        serde_json::from_value(catalog).map_err(|e| AppError::Internal(e.to_string()))?;
    settings.ensure_current(&store)?;
    let (generation, deleted, confirmed) = {
        let previous = bridge.snapshot.read().unwrap_or_else(|p| p.into_inner());
        let same_home = previous.home == settings.home;
        (
            previous.generation.saturating_add(1),
            if same_home {
                previous.deleted.clone()
            } else {
                HashSet::new()
            },
            if same_home {
                previous
                    .confirmed
                    .iter()
                    .filter(|(_, action)| action.as_str() == "delete")
                    .map(|(id, action)| (id.clone(), action.clone()))
                    .collect()
            } else {
                HashMap::new()
            },
        )
    };
    *bridge.snapshot.write().unwrap_or_else(|p| p.into_inner()) = Snapshot {
        connected: true,
        home: settings.home.clone(),
        threads: threads.into_iter().map(|t| (t.id.clone(), t)).collect(),
        generation,
        deleted,
        confirmed,
        ..Snapshot::default()
    };
    for event in notifications {
        bridge.apply_notification(&event);
    }
    let (tx, mut rx) = mpsc::channel::<Evaluation>(32);
    *bridge.sender.write().unwrap_or_else(|p| p.into_inner()) = Some(tx);
    bridge.publish(app, true);
    let mut next_id = 4;
    let mut pending: HashMap<
        u64,
        (
            oneshot::Sender<Result<Value, AppError>>,
            tokio::time::Instant,
        ),
    > = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut catalog_dirty = false;
    let mut catalog_refresh: Option<(u64, u64, u64, tokio::time::Instant)> = None;
    loop {
        tokio::select! {
            request = rx.recv() => {
                let Some(request) = request else { break; };
                settings.ensure_current(&store)?;
                let id = next_id; next_id += 1;
                socket.send(Message::Text(evaluation_request(id, &request.expression).to_string())).await.map_err(|e| AppError::Internal(e.to_string()))?;
                pending.insert(id, (request.result, tokio::time::Instant::now()+Duration::from_secs(29)));
            },
            message = socket.next() => {
                let Some(message) = message else { break; };
                match message.map_err(|e| AppError::Internal(e.to_string()))? {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(&text).map_err(|e| AppError::Internal(e.to_string()))?;
                        if matches!(value.get("method").and_then(Value::as_str),Some("Inspector.detached"|"Runtime.executionContextsCleared")) {break;}
                        if let Some(event) = binding_event(&value) {
                            catalog_dirty |= bridge.needs_catalog_refresh(&event);
                            let catalog_changed = bridge.apply_notification(&event);
                            bridge.publish(app, catalog_changed);
                        }
                        if let Some(id) = value.get("id").and_then(Value::as_u64) {
                            if catalog_refresh.is_some_and(|(refresh_id, _, _, _)| refresh_id == id) {
                                if let Some((_, generation, revision, _)) = catalog_refresh.take() {
                                    let threads: Vec<NativeThread> = serde_json::from_value(desktop_cdp::parse_evaluation_response(&value)?).map_err(|_| AppError::Internal("Codex 会话目录响应无效。".into()))?;
                                    {
                                        let mut snapshot = bridge.snapshot.write().unwrap_or_else(|p| p.into_inner());
                                        replace_catalog(&mut snapshot, generation, revision, threads)?;
                                    }
                                    bridge.publish(app, true);
                                }
                            } else if let Some((result, _)) = pending.remove(&id) { let _ = result.send(desktop_cdp::parse_evaluation_response(&value)); }
                        }
                    },
                    Message::Ping(data) => {socket.send(Message::Pong(data)).await.map_err(|e| AppError::Internal(e.to_string()))?;},
                    Message::Close(_) => break,
                    _ => {},
                }
            },
            _ = tick.tick() => {
                settings.ensure_current(&store)?;
                let now = tokio::time::Instant::now();
                if catalog_refresh.is_some_and(|(_, _, _, deadline)| deadline <= now) {
                    return Err(AppError::Internal("核对新会话目录超时，正在重新连接。".into()));
                }
                if catalog_dirty && catalog_refresh.is_none() {
                    let (generation, revision) = {
                        let snapshot = bridge.snapshot.read().unwrap_or_else(|p| p.into_inner());
                        (snapshot.generation, snapshot.revision)
                    };
                    let id = next_id; next_id += 1;
                    socket.send(Message::Text(evaluation_request(id, "window.__codexToolsSessionsV1.list()").to_string())).await.map_err(|error| AppError::Internal(error.to_string()))?;
                    catalog_refresh = Some((id, generation, revision, now + Duration::from_secs(30)));
                    catalog_dirty = false;
                }
                let expired: Vec<_> = pending.iter().filter(|(_, (_, deadline))| *deadline <= now).map(|(id, _)| *id).collect();
                for id in expired { if let Some((result, _)) = pending.remove(&id) { let _ = result.send(Err(AppError::Internal("Codex 响应超时；请核对操作是否生效。".into()))); } }
            },
        }
    }
    Ok(())
}

async fn initialize_connection<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    notifications: &mut Vec<Value>,
) -> Result<(), AppError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // Enabling the Runtime domain and installing the binding must both succeed;
    // otherwise the catalog would appear connected without lifecycle notifications.
    for request in [
        json!({"id":0,"method":"Runtime.enable"}),
        json!({"id":1,"method":"Runtime.addBinding","params":{"name":BINDING}}),
    ] {
        socket
            .send(Message::Text(request.to_string()))
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let response = read_cdp_response(
            socket,
            request["id"].as_u64().unwrap_or_default(),
            notifications,
        )
        .await?;
        validate_setup_response(&response)?;
    }
    socket
        .send(Message::Text(evaluation_request(2, SCRIPT).to_string()))
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let response = read_cdp_response(socket, 2, notifications).await?;
    if desktop_cdp::parse_evaluation_response(&response)? != Value::Bool(true) {
        return Err(AppError::Internal(
            "Codex 会话同步脚本未完成初始化。".into(),
        ));
    }
    Ok(())
}

fn validate_setup_response(response: &Value) -> Result<(), AppError> {
    if let Some(error) = response.get("error") {
        return Err(AppError::Internal(format!(
            "Codex 调试连接初始化失败：{}",
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("未知错误")
        )));
    }
    if !response.get("result").is_some_and(Value::is_object) {
        return Err(AppError::Internal("Codex 调试初始化响应格式无效。".into()));
    }
    Ok(())
}

async fn read_cdp_response<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    id: u64,
    notifications: &mut Vec<Value>,
) -> Result<Value, AppError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = socket.next().await {
        match message.map_err(|e| AppError::Internal(e.to_string()))? {
            Message::Text(text) => {
                let value: Value =
                    serde_json::from_str(&text).map_err(|e| AppError::Internal(e.to_string()))?;
                if matches!(
                    value.get("method").and_then(Value::as_str),
                    Some("Inspector.detached" | "Runtime.executionContextsCleared")
                ) {
                    return Err(AppError::Internal(
                        "Codex 页面已重载或调试连接已中断。".into(),
                    ));
                }
                if let Some(event) = binding_event(&value) {
                    notifications.push(event);
                }
                if value.get("id").and_then(Value::as_u64) == Some(id) {
                    return Ok(value);
                }
            }
            Message::Ping(data) => {
                socket
                    .send(Message::Pong(data))
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Err(AppError::Internal("Codex 连接已关闭。".into()))
}

fn evaluation_request(id: u64, expression: &str) -> Value {
    json!({"id":id,"method":"Runtime.evaluate","params":{"expression":expression,"awaitPromise":true,"returnByValue":true}})
}

fn binding_event(value: &Value) -> Option<Value> {
    if value.get("method")?.as_str()? != "Runtime.bindingCalled"
        || value.pointer("/params/name")?.as_str()? != BINDING
    {
        return None;
    }
    serde_json::from_str(value.pointer("/params/payload")?.as_str()?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_active_thread_absence_is_not_deletion_evidence() {
        let bridge = SessionBridge::default();
        bridge.snapshot.write().unwrap().connected = true;
        let ids = vec!["empty".to_string()];
        assert!(!bridge.applied("delete", "empty", &ids));
        bridge.apply_notification(&json!({"method":"thread/deleted","threadId":"empty"}));
        assert!(bridge.applied("delete", "empty", &ids));
        assert!(!bridge.applied("delete", "empty", &["empty".into(), "child".into()]));
    }

    #[test]
    fn native_success_proves_empty_unarchive_without_catalog_membership() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path();
        let bridge = SessionBridge::default();
        {
            let mut snapshot = bridge.snapshot.write().unwrap();
            snapshot.connected = true;
            snapshot.home = home.to_path_buf();
        }
        let outcome = json!({"dispatched":true,"value":{"thread":{"id":"empty","preview":"","path":home.join("sessions/empty.jsonl")}}});
        assert!(bridge.record_success("unarchive", "empty", &["empty".into()], home, &outcome));
        assert!(
            !bridge
                .snapshot
                .read()
                .unwrap()
                .threads
                .contains_key("empty")
        );
        assert!(!outcome_confirms("unarchive", "other", home, &outcome));
        let archived = json!({"dispatched":true,"value":{"thread":{"id":"empty","path":home.join("archived_sessions/empty.jsonl")}}});
        assert!(!outcome_confirms("unarchive", "empty", home, &archived));
        let traversal = json!({"dispatched":true,"value":{"thread":{"id":"empty","path":home.join("sessions/../other.jsonl")}}});
        assert!(!outcome_confirms("unarchive", "empty", home, &traversal));
        assert!(!outcome_confirms(
            "delete",
            "empty",
            home,
            &json!({"dispatched":true,"error":"lost","value":{}})
        ));
        assert!(bridge.record_success(
            "delete",
            "empty",
            &["empty".into(), "child".into()],
            home,
            &json!({"dispatched":true,"value":{}})
        ));
        assert!(bridge.applied("delete", "empty", &["empty".into(), "child".into()]));
    }

    #[tokio::test]
    async fn uncertain_empty_unarchive_uses_native_metadata_read_without_turns() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_path_buf();
        let bridge = SessionBridge::default();
        {
            let mut snapshot = bridge.snapshot.write().unwrap();
            snapshot.connected = true;
            snapshot.home = home.clone();
        }
        let (tx, mut rx) = mpsc::channel::<Evaluation>(1);
        *bridge.sender.write().unwrap() = Some(tx);
        let responder = tokio::spawn(async move {
            let request = rx.recv().await.unwrap();
            assert!(request.expression.contains("thread/read"));
            assert!(request.expression.contains("\"includeTurns\":false"));
            request.result.send(Ok(json!({"thread":{"id":"empty","preview":"","path":home.join("sessions/empty.jsonl"),"status":{"type":"idle"}}}))).unwrap();
        });
        assert!(
            bridge
                .confirm_applied("unarchive", "empty", &["empty".into()], temp.path())
                .await
                .unwrap()
        );
        responder.await.unwrap();
        assert!(
            !bridge
                .confirm_applied("delete", "empty", &["empty".into()], temp.path())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn delete_preview_reuses_scope_runtime_without_a_second_loaded_query() {
        let temp = tempfile::tempdir().unwrap();
        let bridge = SessionBridge::default();
        {
            let mut snapshot = bridge.snapshot.write().unwrap();
            snapshot.connected = true;
            snapshot.home = temp.path().to_path_buf();
            snapshot
                .threads
                .insert("root".into(), thread("root", None, "idle"));
        }
        let (tx, mut rx) = mpsc::channel::<Evaluation>(1);
        *bridge.sender.write().unwrap() = Some(tx);
        let responder =
            tokio::spawn(async move {
                let request = rx.recv().await.unwrap();
                assert_eq!(
                    request.expression,
                    "window.__codexToolsSessionsV1.affectedScope(\"root\")"
                );
                request.result.send(Ok(json!({
                "threads":[{"id":"root","title":"root"},{"id":"hidden-child","title":"hidden"}],
                "blockedReason":"会话或派生会话正在运行，请等待完成后再操作。"
            }))).unwrap();
            });
        let (threads, blocked) = bridge.affected("root", temp.path()).await.unwrap();
        assert_eq!(
            threads
                .iter()
                .map(|thread| thread.id.as_str())
                .collect::<Vec<_>>(),
            ["root", "hidden-child"]
        );
        assert!(blocked.unwrap().contains("正在运行"));
        responder.await.unwrap();
    }

    #[test]
    fn empty_started_thread_stays_hidden_until_native_catalog_exposes_first_input() {
        let bridge = SessionBridge::default();
        {
            let mut snapshot = bridge.snapshot.write().unwrap();
            snapshot.connected = true;
            snapshot.generation = 1;
        }
        bridge.apply_notification(&json!({"method":"thread/started","thread":{"id":"empty","preview":"","status":{"type":"idle"}}}));
        assert!(!bridge.snapshot.read().unwrap().threads["empty"].is_list_visible());
        let active =
            json!({"method":"thread/status/changed","threadId":"empty","status":{"type":"active"}});
        assert!(bridge.needs_catalog_refresh(&active));
        bridge.apply_notification(&active);
        assert!(!bridge.needs_catalog_refresh(&active));
        assert!(!bridge.needs_catalog_refresh(&json!({"method":"thread/status/changed","threadId":"empty","status":{"type":"active","activeFlags":["waitingOnApproval"]}})));
        assert!(
            bridge.needs_catalog_refresh(&json!({"method":"turn/completed","threadId":"empty"}))
        );
        let mut populated = thread("empty", None, "idle");
        populated.preview = Some("first user input".into());
        populated.path = Some("sessions/empty.jsonl".into());
        let mut snapshot = bridge.snapshot.write().unwrap();
        let revision = snapshot.revision;
        replace_catalog(&mut snapshot, 1, revision, vec![populated]).unwrap();
        assert!(snapshot.threads["empty"].is_list_visible());
        drop(snapshot);
        assert!(!bridge.needs_catalog_refresh(&active));
    }

    #[test]
    fn rename_notifications_update_delete_preview_metadata_and_survive_refresh() {
        let bridge = SessionBridge::default();
        {
            let mut snapshot = bridge.snapshot.write().unwrap();
            snapshot.connected = true;
            snapshot.generation = 1;
            snapshot
                .threads
                .insert("root".into(), thread("root", None, "idle"));
        }
        assert!(bridge.apply_notification(
            &json!({"method":"thread/name/updated","threadId":"root","name":"新名称"})
        ));
        assert_eq!(
            bridge
                .thread("root", std::path::Path::new(""))
                .unwrap()
                .title,
            "新名称"
        );
        let mut snapshot = bridge.snapshot.write().unwrap();
        replace_catalog(&mut snapshot, 1, 0, vec![thread("root", None, "idle")]).unwrap();
        assert_eq!(snapshot.threads["root"].title, "新名称");
        update_threads(
            &mut snapshot.threads,
            &json!({"method":"thread/name/updated","threadId":"root","name":null}),
        );
        assert_eq!(snapshot.threads["root"].title, "preview root");
        update_threads(
            &mut snapshot.threads,
            &json!({"method":"thread/name/updated","threadId":"root","name":""}),
        );
        assert_eq!(snapshot.threads["root"].title, "preview root");
    }

    #[test]
    fn connection_settings_change_when_home_or_debug_port_changes() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("data")).unwrap();
        let initial = ConnectionSettings::read(&store).unwrap();
        initial.ensure_current(&store).unwrap();
        store
            .update(|state| {
                state.codex.home = temp.path().join("different-home").display().to_string();
                Ok(())
            })
            .unwrap();
        assert!(initial.ensure_current(&store).is_err());

        let changed_home = ConnectionSettings::read(&store).unwrap();
        store.save_last_debug_port(54321).unwrap();
        assert!(changed_home.ensure_current(&store).is_err());
        let changed_port = ConnectionSettings::read(&store).unwrap();
        store
            .settings_save_codex_app_path(Some("different-app.exe".into()))
            .unwrap();
        changed_port.ensure_current(&store).unwrap();
    }

    #[tokio::test]
    async fn setup_rejects_runtime_and_binding_errors_before_script_injection() {
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        for failed_id in [0, 1] {
            let (client, server) = tokio::io::duplex(64 * 1024);
            let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
            let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
            let responder = tokio::spawn(async move {
                let mut methods = Vec::new();
                for id in 0..=failed_id {
                    let Message::Text(text) = server.next().await.unwrap().unwrap() else {
                        panic!("expected a CDP request");
                    };
                    let request: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(request["id"], id);
                    methods.push(request["method"].as_str().unwrap().to_string());
                    let response = if id == failed_id {
                        json!({"id":id,"error":{"code":-32601,"message":"setup unavailable"}})
                    } else {
                        json!({"id":id,"result":{}})
                    };
                    server
                        .send(Message::Text(response.to_string()))
                        .await
                        .unwrap();
                }
                methods
            });
            let error = initialize_connection(&mut client, &mut Vec::new())
                .await
                .unwrap_err();
            assert!(error.to_string().contains("setup unavailable"));
            let methods = responder.await.unwrap();
            assert_eq!(methods.len(), failed_id as usize + 1);
            assert!(!methods.iter().any(|method| method == "Runtime.evaluate"));
        }
    }

    #[tokio::test]
    async fn setup_waits_for_acknowledgements_and_buffers_notifications() {
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        let (client, server) = tokio::io::duplex(64 * 1024);
        let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let responder = tokio::spawn(async move {
            let mut methods = Vec::new();
            for id in 0..=2 {
                let Message::Text(text) = server.next().await.unwrap().unwrap() else {
                    panic!("expected a CDP request");
                };
                let request: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(request["id"], id);
                methods.push(request["method"].as_str().unwrap().to_string());
                if id == 1 {
                    server
                        .send(Message::Text(json!({
                            "method":"Runtime.bindingCalled",
                            "params":{"name":BINDING,"payload":json!({"method":"thread/deleted","threadId":"removed"}).to_string()}
                        }).to_string()))
                        .await
                        .unwrap();
                }
                let response = if id == 2 {
                    json!({"id":id,"result":{"result":{"type":"boolean","value":true}}})
                } else {
                    json!({"id":id,"result":{}})
                };
                server
                    .send(Message::Text(response.to_string()))
                    .await
                    .unwrap();
            }
            methods
        });
        let mut notifications = Vec::new();
        initialize_connection(&mut client, &mut notifications)
            .await
            .unwrap();
        assert_eq!(
            responder.await.unwrap(),
            ["Runtime.enable", "Runtime.addBinding", "Runtime.evaluate"]
        );
        assert_eq!(
            notifications,
            [json!({"method":"thread/deleted","threadId":"removed"})]
        );
        assert!(validate_setup_response(&json!({"id":0})).is_err());
    }

    #[test]
    fn refresh_replays_concurrent_deletion_and_rejects_old_connection_results() {
        let bridge = SessionBridge::default();
        {
            let mut snapshot = bridge.snapshot.write().unwrap();
            snapshot.connected = true;
            snapshot.generation = 7;
            snapshot
                .threads
                .insert("deleted".into(), thread("deleted", None, "idle"));
        }
        bridge.apply_notification(&json!({"method":"thread/deleted","threadId":"deleted"}));
        bridge.apply_notification(&json!({"method":"thread/status/changed","threadId":"running","status":{"type":"active"}}));
        let mut snapshot = bridge.snapshot.write().unwrap();
        replace_catalog(
            &mut snapshot,
            7,
            0,
            vec![
                thread("deleted", None, "idle"),
                thread("running", None, "idle"),
            ],
        )
        .unwrap();
        assert!(!snapshot.threads.contains_key("deleted"));
        assert_eq!(snapshot.threads["running"].runtime().state, "running");
        assert!(replace_catalog(&mut snapshot, 6, 0, vec![]).is_err());
        assert!(snapshot.threads.contains_key("running"));
    }
    fn thread(id: &str, parent: Option<&str>, state: &str) -> NativeThread {
        NativeThread {
            id: id.into(),
            title: id.into(),
            fallback_title: format!("preview {id}"),
            preview: None,
            path: None,
            status: json!({"type":state}),
            parent: parent.map(str::to_string),
            archived: false,
            cwd: String::new(),
            provider: String::new(),
            updated_at: 0,
        }
    }
    #[test]
    fn loaded_idle_is_operable_and_active_approval_is_running() {
        assert_eq!(thread("idle", None, "idle").runtime().state, "idle");
        assert_eq!(
            thread("unloaded", None, "notLoaded").runtime().state,
            "idle"
        );
        assert_eq!(thread("active", None, "active").runtime().state, "running");
        assert_eq!(
            thread("error", None, "systemError").runtime().state,
            "unknown"
        );
    }
    #[test]
    fn descendants_include_archived_deep_children_but_not_unrelated_threads_or_cycles() {
        let threads = vec![
            thread("a", Some("root"), "idle"),
            thread("b", Some("a"), "active"),
            thread("other", Some("different"), "idle"),
            thread("cycle", Some("cycle"), "idle"),
        ];
        assert_eq!(
            subtree("root", &threads)
                .iter()
                .map(|t| t.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }
    #[test]
    fn notifications_remove_catalog_entries_and_update_runtime() {
        let bridge = SessionBridge::default();
        bridge
            .snapshot
            .write()
            .unwrap()
            .threads
            .insert("id".into(), thread("id", None, "idle"));
        bridge.apply_notification(
            &json!({"method":"thread/status/changed","threadId":"id","status":{"type":"active"}}),
        );
        assert_eq!(
            bridge.snapshot.read().unwrap().threads["id"]
                .runtime()
                .state,
            "running"
        );
        bridge.apply_notification(&json!({"method":"thread/archived","threadId":"id"}));
        assert!(bridge.snapshot.read().unwrap().threads["id"].archived);
        bridge.apply_notification(&json!({"method":"thread/unarchived","threadId":"id"}));
        assert!(!bridge.snapshot.read().unwrap().threads["id"].archived);
        bridge.apply_notification(&json!({"method":"thread/deleted","threadId":"id"}));
        assert!(!bridge.snapshot.read().unwrap().threads.contains_key("id"));
    }
}
