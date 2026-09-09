use crate::{
    json_store::{JsonStore, sync_parent_directory},
    models::*,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, RwLock, RwLockReadGuard},
};

const MAX_SAVED_PROVIDERS: usize = 500;
pub(crate) const MAX_SAVED_OPENAI_ACCOUNTS: usize = 500;
const MAX_EMAIL_CHARS: usize = 320;
const STATE_TRANSACTION_FILE: &str = "state-transaction.json";
const COMPLETED_STATE_TRANSACTION_FILE: &str = ".state-transaction.completed";
const STATE_TRANSACTION_VERSION: u32 = 1;
const MAX_STATE_TRANSACTION_BYTES: u64 = 129 * 1024 * 1024;
#[cfg(test)]
const MAX_APP_DATA_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct AppFile {
    #[serde(default)]
    codex: CodexPreferences,
    #[serde(default)]
    active: ActiveState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ConnectionsFile {
    #[serde(default)]
    providers: Vec<ProviderProfile>,
    #[serde(default)]
    official_accounts: Vec<StoredOfficialAccount>,
    #[serde(default)]
    credential_refresh: BTreeMap<String, CredentialRefreshState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CredentialsFile {
    #[serde(default)]
    provider_api_keys: BTreeMap<String, String>,
    #[serde(default)]
    provider_headers: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StateTransaction {
    version: u32,
    app: AppFile,
    connections: ConnectionsFile,
    credentials: CredentialsFile,
    deletion_tombstones: DeletionTombstones,
}

pub struct Store {
    root: PathBuf,
    path: PathBuf,
    connections_path: PathBuf,
    credentials_path: PathBuf,
    tombstones_path: PathBuf,
    state: RwLock<AppConfig>,
    persist_mutex: Mutex<()>,
}

/// 标识一次 `/models` 请求所对应的服务源。包含会改变请求目标、鉴权、
/// 协议或 models.dev 匹配结果的字段；不实现 Debug，避免意外输出密钥/请求头。
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ProviderSourceFingerprint {
    id: String,
    name: String,
    base_url: String,
    headers: BTreeMap<String, String>,
    api_type: ProviderApiType,
    api_key: Option<String>,
}

/// Provider 完整持久化快照的不可逆 revision，用于失败回滚前的用户态
/// check-and-rename 检查。与 source fingerprint 不同，它会感知
/// timeout/enabled/模型缓存等任何后续提交。
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ProviderSnapshotRevision([u8; 32]);

impl ProviderSourceFingerprint {
    pub(crate) fn from_provider(provider: &ProviderProfile) -> Self {
        Self {
            id: provider.id.clone(),
            name: provider.name.clone(),
            base_url: provider.base_url.clone(),
            headers: provider.headers.clone(),
            api_type: provider.api_type,
            api_key: provider.api_key.clone(),
        }
    }
}

impl ProviderSnapshotRevision {
    pub(crate) fn from_provider(provider: &ProviderProfile) -> Self {
        use sha2::{Digest, Sha256};

        let serialized = serde_json::to_vec(provider).unwrap_or_default();
        Self(Sha256::digest(serialized).into())
    }
}

impl Store {
    pub fn new() -> anyhow::Result<Self> {
        Self::open(data_root())
    }

    pub fn open(root: PathBuf) -> anyhow::Result<Self> {
        fs::create_dir_all(&root)?;
        secure_directory(&root)?;
        recover_state_transaction(&root)?;
        let path = root.join("app.json");
        let connections_path = root.join("connections.json");
        let credentials_path = root.join("credentials.json");
        let tombstones_path = root.join("deletion_tombstones.json");
        let mut state = load_state_files(&root)?;
        let mut requires_persist = false;
        // 当前目录可能被旧版重写过 connections.json；先清理已被墓碑覆盖的连接，
        // 并修正因此产生的悬空 active 选择。
        requires_persist |= remove_tombstoned_connections(&mut state);

        // 每次加载后都归并当前根目录中已经写入的同身份重复记录；无重复状态
        // 不会触发持久化。
        requires_persist |= merge_current_duplicate_official_accounts(&mut state);
        // 旧算法的估算金额会放大事件，缺失版本字段按 0 处理；加载时必须从持久化
        // 状态撤销，避免页面在下一次重新估算前继续显示错误金额。
        requires_persist |= discard_outdated_quota_estimates(&mut state);

        let files_incomplete = !state_files_complete(
            &path,
            &connections_path,
            &credentials_path,
            &tombstones_path,
        );
        if requires_persist || files_incomplete {
            persist_files(
                &root,
                &path,
                &connections_path,
                &credentials_path,
                &tombstones_path,
                &state,
            )?;
        }
        if files_incomplete {
            for name in [
                "credentials.json",
                "pricing.json",
                "usage.json",
                "sessions.json",
                "cache.json",
            ] {
                JsonStore::ensure_object(&root.join(name))?;
            }
        }
        Ok(Self {
            root,
            path,
            connections_path,
            credentials_path,
            tombstones_path,
            state: RwLock::new(state),
            persist_mutex: Mutex::new(()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_state(&self) -> Result<RwLockReadGuard<'_, AppConfig>, AppError> {
        self.state
            .read()
            .map_err(|_| AppError::Internal("暂时无法读取应用数据，请重启应用后再试。".into()))
    }

    fn current_state_for_update(&self) -> Result<AppConfig, AppError> {
        self.state
            .read()
            .map(|state| state.clone())
            .map_err(|_| AppError::Internal("暂时无法保存应用数据，请重启应用后再试。".into()))
    }

    pub(crate) fn read<T>(&self, project: impl FnOnce(&AppConfig) -> T) -> Result<T, AppError> {
        let state = self.read_state()?;
        Ok(project(&state))
    }

    #[cfg(test)]
    pub fn snapshot(&self) -> Result<AppConfig, AppError> {
        self.read(Clone::clone)
    }

    pub fn codex_home_setting(&self) -> Result<String, AppError> {
        Ok(self.read_state()?.codex.home.clone())
    }

    pub fn codex_app_setting(&self) -> Result<Option<String>, AppError> {
        Ok(self.read_state()?.codex.app_path.clone())
    }

    pub fn settings_save_codex_app_path(&self, path: Option<String>) -> Result<(), AppError> {
        self.update(|state| {
            state.codex.app_path = path
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty());
            Ok(())
        })
    }

    pub fn last_debug_port(&self) -> Result<Option<u16>, AppError> {
        Ok(self.read_state()?.codex.last_debug_port)
    }

    pub fn save_last_debug_port(&self, port: u16) -> Result<(), AppError> {
        self.update(|state| {
            state.codex.last_debug_port = Some(port);
            Ok(())
        })
    }

    pub fn last_managed_model(&self) -> Result<Option<String>, AppError> {
        Ok(self.read_state()?.codex.last_managed_model.clone())
    }

    pub fn save_last_managed_model(&self, model: Option<String>) -> Result<(), AppError> {
        self.update(|state| {
            state.codex.last_managed_model = model;
            Ok(())
        })
    }

    pub fn is_active_provider(&self, id: &str) -> Result<bool, AppError> {
        let state = self.read_state()?;
        Ok(matches!(state.active.kind, ActiveKind::Provider)
            && state.active.provider_id.as_deref() == Some(id))
    }

    pub fn update<T>(
        &self,
        mutate: impl FnOnce(&mut AppConfig) -> Result<T, AppError>,
    ) -> Result<T, AppError> {
        // 串行化“候选状态 -> 落盘 -> 内存提交”的整个事务，避免并发更新
        // 从同一旧快照出发。落盘失败时不更换内存状态，调用方收到错误后
        // 仍能看到与持久化前一致的状态。
        let _transaction = self
            .persist_mutex
            .lock()
            .map_err(|_| AppError::Internal("暂时无法保存应用数据，请重启应用后再试。".into()))?;

        // 候选状态上的变更和 fsync 都不持有状态写锁，读操作在提交前
        // 继续看到稳定的旧状态。
        let current = self.current_state_for_update()?;
        let mut draft = current.clone();
        let result = mutate(&mut draft)?;
        if let Err(error) = persist_files(
            &self.root,
            &self.path,
            &self.connections_path,
            &self.credentials_path,
            &self.tombstones_path,
            &draft,
        ) {
            return Err(AppError::Internal(error.to_string()));
        }

        let mut guard = self
            .state
            .write()
            .map_err(|_| AppError::Internal("暂时无法保存应用数据，请重启应用后再试。".into()))?;
        *guard = draft;
        Ok(result)
    }

    pub fn provider(&self, id: &str) -> Result<ProviderProfile, AppError> {
        let state = self.read_state()?;
        let mut profile = state
            .providers
            .iter()
            .find(|provider| provider.id == id)
            .cloned()
            .ok_or_else(|| AppError::InvalidConfig("第三方 API 服务不存在，请刷新页面。".into()))?;
        mark_active_profile(&state, &mut profile);
        Ok(profile)
    }

    /// 为锁外模型刷新原子捕获持久化 Provider、完整 revision 及开始时的
    /// active 状态。活跃服务用 revision 提交，避免旧响应覆盖后续同源编辑。
    pub(crate) fn provider_model_refresh_snapshot(
        &self,
        id: &str,
    ) -> Result<(ProviderProfile, ProviderSnapshotRevision, bool), AppError> {
        self.read(|state| {
            let provider = state
                .providers
                .iter()
                .find(|provider| provider.id == id)
                .cloned()
                .ok_or_else(|| {
                    AppError::InvalidConfig("第三方 API 服务不存在，请刷新页面。".into())
                })?;
            let revision = ProviderSnapshotRevision::from_provider(&provider);
            let active = matches!(state.active.kind, ActiveKind::Provider)
                && state.active.provider_id.as_deref() == Some(id);
            Ok((provider, revision, active))
        })?
    }

    #[cfg(test)]
    pub fn connections_save_provider(
        &self,
        provider: ProviderProfile,
    ) -> Result<ProviderProfile, AppError> {
        self.connections_save_provider_with_previous(provider, false)
            .map(|(_, saved)| saved)
    }

    pub(crate) fn connections_save_provider_with_previous(
        &self,
        mut provider: ProviderProfile,
        custom_models_explicit: bool,
    ) -> Result<(Option<ProviderProfile>, ProviderProfile), AppError> {
        if provider.id.trim().is_empty() {
            provider.id = uuid::Uuid::new_v4().to_string();
        }
        self.update(move |state| {
            // existing 查找、脱敏字段合并、模型缓存决策和最终替换必须处于
            // 同一 Store 事务，不能让锁外旧快照覆盖刚完成的 revision 检查与替换。
            let existing = state
                .providers
                .iter()
                .find(|value| value.id == provider.id)
                .cloned();
            let is_new = existing.is_none();
            if let Some(existing) = existing.as_ref() {
                preserve_redacted_headers(&mut provider.headers, &existing.headers);
                // 前端返回的是脱敏后的 api_key（None），保留已保存的 Key。
                if provider
                    .api_key
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    provider.api_key = existing.api_key.clone();
                }
            }
            // `model` 仅用于兼容旧版 connections.json 的反序列化。模型选择现在
            // 完全由服务 `/models` 返回的 available_models 和 Codex 当前配置决定。
            provider.model.clear();
            provider.normalize_and_validate()?;
            let model_source_changed = existing.as_ref().is_none_or(|existing| {
                existing.name != provider.name
                    || existing.base_url != provider.base_url
                    || existing.api_key != provider.api_key
                    || existing.headers != provider.headers
                    || existing.api_type != provider.api_type
            });
            if let Some(existing) = existing.as_ref() {
                if model_source_changed {
                    provider.model_context_windows.clear();
                    provider.available_models.clear();
                    // 模型源变化只使 API 目录缓存失效；用户显式选择的子集必须保留，
                    // 等新 `/models` 返回后再取交集，绝不能静默扩大为全选。
                    provider.models_dev_meta.clear();
                    // 自定义模型属于用户数据，不随服务源修改而清空。
                    if provider.custom_models.is_empty() && !custom_models_explicit {
                        provider.custom_models = existing.custom_models.clone();
                    }
                } else {
                    if provider.model_context_windows.is_empty() {
                        provider.model_context_windows = existing.model_context_windows.clone();
                    }
                    if provider.available_models.is_empty() {
                        provider.available_models = existing.available_models.clone();
                    }
                    // selected_models 是用户可编辑字段：None 明确表示清除旧筛选、
                    // 使用全部有效模型，不能像后端缓存字段一样从旧记录回填。
                    if provider.custom_models.is_empty() && !custom_models_explicit {
                        provider.custom_models = existing.custom_models.clone();
                    }
                    if provider.models_dev_meta.is_empty() {
                        provider.models_dev_meta = existing.models_dev_meta.clone();
                    }
                }
            }
            let now = chrono::Utc::now().timestamp();
            if provider.created_at == 0 {
                provider.created_at = existing.as_ref().map_or(now, |value| value.created_at);
            }
            provider.updated_at = now;
            provider.active = false;
            provider.has_api_key = provider.api_key.is_some();

            if matches!(state.active.kind, ActiveKind::Provider)
                && state.active.provider_id.as_deref() == Some(provider.id.as_str())
                && !provider.enabled
            {
                return Err(AppError::InvalidConfig(
                    "正在使用此服务，切换到其他账号或服务后才能停用。".into(),
                ));
            }
            if let Some(existing) = state
                .providers
                .iter_mut()
                .find(|value| value.id == provider.id)
            {
                *existing = provider.clone();
            } else {
                if is_new && state.providers.len() >= MAX_SAVED_PROVIDERS {
                    return Err(AppError::InvalidConfig(
                        "最多可保存 500 个第三方 API 服务，请先删除不再使用的服务。".into(),
                    ));
                }
                state.providers.push(provider.clone());
            }
            // 相同 id 的显式保存代表用户主动恢复，不能继续让旧删除操作阻止它。
            state.deletion_tombstones.provider_ids.remove(&provider.id);
            Ok((existing, provider))
        })
    }

    pub fn connections_delete_provider(&self, id: &str) -> Result<(), AppError> {
        self.update(|state| {
            if state.active.provider_id.as_deref() == Some(id) {
                return Err(AppError::InvalidConfig(
                    "正在使用此服务，切换后才能删除。".into(),
                ));
            }
            let before = state.providers.len();
            state.providers.retain(|value| value.id != id);
            if before == state.providers.len() {
                return Err(AppError::InvalidConfig(
                    "第三方 API 服务不存在，可能已被删除。".into(),
                ));
            }
            state.deletion_tombstones.provider_ids.insert(id.to_owned());
            Ok(())
        })
    }

    /// 保存从服务 `/models` 接口读取到的模型上下文窗口，供模型目录使用。
    /// 保存服务 `/models` 接口返回的可用模型、上下文窗口，以及 models.dev
    /// 精确匹配的模型元数据，供模型目录使用。
    pub(crate) fn update_provider_models_if_source_matches(
        &self,
        id: &str,
        expected_source: &ProviderSourceFingerprint,
        expected_revision: Option<&ProviderSnapshotRevision>,
        models: Vec<String>,
        windows: BTreeMap<String, u64>,
        meta: BTreeMap<String, ProviderModelsDevMeta>,
    ) -> Result<Option<ProviderSnapshotRevision>, AppError> {
        self.update(|state| {
            let Some(provider) = state.providers.iter_mut().find(|value| value.id == id) else {
                return Ok(None);
            };
            if ProviderSourceFingerprint::from_provider(provider) != *expected_source {
                return Ok(None);
            }
            if expected_revision.is_some_and(|revision| {
                ProviderSnapshotRevision::from_provider(provider) != *revision
            }) {
                return Ok(None);
            }
            provider.available_models = models;
            provider.model_context_windows = windows;
            provider.models_dev_meta = meta;
            if let Some(selected) = provider.selected_models.as_mut() {
                selected.retain(|model| {
                    provider.available_models.contains(model)
                        || provider.custom_models.contains(model)
                });
            }
            // 刷新模型列表时顺带清理尚未经过“保存服务”迁移的旧默认模型。
            provider.model.clear();
            Ok(Some(ProviderSnapshotRevision::from_provider(provider)))
        })
    }

    pub(crate) fn provider_source_matches(
        &self,
        id: &str,
        expected_source: &ProviderSourceFingerprint,
    ) -> Result<bool, AppError> {
        self.read(|state| {
            state
                .providers
                .iter()
                .find(|provider| provider.id == id)
                .is_some_and(|provider| {
                    ProviderSourceFingerprint::from_provider(provider) == *expected_source
                })
        })
    }

    /// active Provider 保存失败时使用的精确回滚路径。它不经过普通保存的
    /// 合并/清缓存逻辑，完整恢复旧快照；仅当前完整 Provider revision 仍与
    /// 预期一致时提交，避免较早请求覆盖随后完成的任何编辑或缓存更新。
    #[cfg(test)]
    pub(crate) fn provider_snapshot_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProviderSnapshotRevision>, AppError> {
        self.read(|state| {
            state
                .providers
                .iter()
                .find(|provider| provider.id == id)
                .map(ProviderSnapshotRevision::from_provider)
        })
    }

    pub(crate) fn restore_provider_snapshot_if_revision_matches(
        &self,
        expected_revision: &ProviderSnapshotRevision,
        previous: &ProviderProfile,
    ) -> Result<bool, AppError> {
        self.update(|state| {
            let Some(current) = state
                .providers
                .iter_mut()
                .find(|provider| provider.id == previous.id)
            else {
                return Ok(false);
            };
            if ProviderSnapshotRevision::from_provider(current) != *expected_revision {
                return Ok(false);
            }
            let mut restored = previous.clone();
            restored.active = false;
            restored.has_api_key = restored.api_key.is_some();
            *current = restored;
            Ok(true)
        })
    }

    pub fn provider_overview(&self) -> Result<ProviderOverview, AppError> {
        let state = self.read_state()?;
        let providers = state
            .providers
            .iter()
            .cloned()
            .map(|mut profile| {
                mark_active_profile(&state, &mut profile);
                profile.redacted()
            })
            .collect();
        let official_accounts = state
            .official_accounts
            .iter()
            .map(|account| {
                let active = matches!(state.active.kind, ActiveKind::Official)
                    && state.active.account_id.as_deref() == Some(account.id.as_str());
                account.view_with_credential_refresh(
                    active,
                    state
                        .credential_refresh
                        .get(&account.id)
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .collect();
        Ok(ProviderOverview {
            providers,
            official_accounts,
        })
    }

    pub fn activate(&self, provider_id: &str) -> Result<(), AppError> {
        self.update(|state| {
            let provider = state
                .providers
                .iter_mut()
                .find(|value| value.id == provider_id)
                .ok_or_else(|| {
                    AppError::InvalidConfig("第三方 API 服务不存在，请刷新页面。".into())
                })?;
            if !provider.enabled {
                return Err(AppError::InvalidConfig(
                    "此服务已停用，请先编辑并启用它。".into(),
                ));
            }
            if provider
                .api_key
                .as_deref()
                .is_none_or(|key| key.trim().is_empty())
            {
                return Err(AppError::InvalidConfig(
                    "此服务还没有 API Key，请先编辑并填写。".into(),
                ));
            }
            state.active = ActiveState {
                kind: ActiveKind::Provider,
                provider_id: Some(provider_id.to_owned()),
                account_id: None,
            };
            Ok(())
        })
    }

    /// 会话修复未完成导致切换回滚时，把应用激活状态恢复为切换前的连接。
    /// 目标连接必须在应用中仍然存在，避免把状态指向已删除的服务或账号。
    pub(crate) fn restore_active_state(&self, previous: &ActiveState) -> Result<(), AppError> {
        self.update(|state| {
            match previous.kind {
                ActiveKind::Provider => {
                    let Some(id) = previous.provider_id.as_deref() else {
                        return Err(AppError::InvalidConfig(
                            "原连接信息不完整，无法恢复。".into(),
                        ));
                    };
                    if !state.providers.iter().any(|provider| provider.id == id) {
                        return Err(AppError::InvalidConfig(
                            "原第三方 API 服务已不存在，无法恢复。".into(),
                        ));
                    }
                }
                ActiveKind::Official => {
                    let Some(id) = previous.account_id.as_deref() else {
                        return Err(AppError::InvalidConfig(
                            "原账号信息不完整，无法恢复。".into(),
                        ));
                    };
                    if !state
                        .official_accounts
                        .iter()
                        .any(|account| account.id == id)
                    {
                        return Err(AppError::InvalidConfig(
                            "原 OpenAI 账号已不存在，无法恢复。".into(),
                        ));
                    }
                }
                ActiveKind::None => {}
            }
            state.active = previous.clone();
            Ok(())
        })
    }

    pub fn official_account_view(&self, id: &str) -> Result<OfficialAccountView, AppError> {
        let state = self.read_state()?;
        let account = state
            .official_accounts
            .iter()
            .find(|account| account.id == id)
            .ok_or_else(|| AppError::InvalidConfig("OpenAI 账号不存在，可能已被删除。".into()))?;
        let active = matches!(state.active.kind, ActiveKind::Official)
            && state.active.account_id.as_deref() == Some(account.id.as_str());
        Ok(account.view_with_credential_refresh(
            active,
            state
                .credential_refresh
                .get(id)
                .cloned()
                .unwrap_or_default(),
        ))
    }

    pub(crate) fn official_accounts_for_maintenance(
        &self,
    ) -> Result<Vec<(StoredOfficialAccount, CredentialRefreshState, bool)>, AppError> {
        self.read(|state| {
            Ok(state
                .official_accounts
                .iter()
                .cloned()
                .map(|account| {
                    let active = matches!(state.active.kind, ActiveKind::Official)
                        && state.active.account_id.as_deref() == Some(account.id.as_str());
                    let refresh = state
                        .credential_refresh
                        .get(&account.id)
                        .cloned()
                        .unwrap_or_default();
                    (account, refresh, active)
                })
                .collect())
        })?
    }

    pub(crate) fn save_credential_refresh_state(
        &self,
        id: &str,
        refresh: CredentialRefreshState,
    ) -> Result<(), AppError> {
        self.update(|state| {
            if !state
                .official_accounts
                .iter()
                .any(|account| account.id == id)
            {
                return Err(AppError::InvalidConfig(
                    "OpenAI 账号不存在，可能已被删除。".into(),
                ));
            }
            state.credential_refresh.insert(id.to_owned(), refresh);
            Ok(())
        })
    }

    /// Returns the sensitive stored record for backend-only auth operations.
    /// Tauri commands must return `official_account_view` instead.
    pub fn official_account(&self, id: &str) -> Result<StoredOfficialAccount, AppError> {
        self.read_state()?
            .official_accounts
            .iter()
            .find(|account| account.id == id)
            .cloned()
            .ok_or_else(|| AppError::InvalidConfig("OpenAI 账号不存在，可能已被删除。".into()))
    }

    pub fn save_official_account(
        &self,
        account: &StoredOfficialAccount,
    ) -> Result<StoredOfficialAccount, AppError> {
        self.save_official_accounts(std::slice::from_ref(account))?
            .into_iter()
            .next()
            .ok_or_else(|| AppError::Internal("OpenAI 账号保存结果为空。".into()))
    }

    /// Updates credentials obtained by refreshing an existing OpenAI account.
    ///
    /// Account remarks and quota snapshots are edited through separate paths.
    /// Resolve them from the latest durable record at commit time instead of
    /// trusting the potentially stale account snapshot used for the network
    /// request.
    pub fn save_refreshed_official_account(
        &self,
        id: &str,
        refreshed: &StoredOfficialAccount,
    ) -> Result<StoredOfficialAccount, AppError> {
        let mut refreshed = refreshed.clone();
        normalize_official_account(&mut refreshed)?;
        self.update(|state| {
            let existing = state
                .official_accounts
                .iter_mut()
                .find(|account| account.id == id)
                .ok_or_else(|| {
                    AppError::InvalidConfig("OpenAI 账号不存在，可能已被删除。".into())
                })?;
            if !official_account_identity_matches(existing, &refreshed) {
                return Err(AppError::InvalidConfig(
                    "OpenAI 返回的凭据属于其他账号，请重新进行官方授权。".into(),
                ));
            }

            refreshed.id = existing.id.clone();
            refreshed.remark = existing.remark.clone();
            refreshed.quota = existing.quota.clone();
            refreshed.created_at = existing.created_at;
            refreshed.updated_at = chrono::Utc::now().timestamp();
            *existing = refreshed.clone();
            state.credential_refresh.remove(id);
            Ok(refreshed)
        })
    }

    pub fn save_official_accounts(
        &self,
        accounts: &[StoredOfficialAccount],
    ) -> Result<Vec<StoredOfficialAccount>, AppError> {
        let mut incoming_accounts = accounts.to_vec();
        for account in &mut incoming_accounts {
            normalize_official_account(account)?;
        }
        let now = chrono::Utc::now().timestamp();

        self.update(move |state| {
            ensure_official_account_capacity(&state.official_accounts, &incoming_accounts)?;
            let mut saved_accounts = Vec::with_capacity(incoming_accounts.len());
            for mut incoming in incoming_accounts {
                // 相同 canonical 身份的显式登录是主动恢复，即使本地 record id 与
                // 已删除的历史记录不同，也必须允许其重新出现。
                state
                    .deletion_tombstones
                    .official_account_ids
                    .remove(&canonical_official_account_id(&incoming));
                let active_account_id = matches!(state.active.kind, ActiveKind::Official)
                    .then_some(state.active.account_id.as_deref())
                    .flatten();
                let existing_index = state
                    .official_accounts
                    .iter()
                    .position(|saved| {
                        active_account_id == Some(saved.id.as_str())
                            && official_account_identity_matches(saved, &incoming)
                    })
                    .or_else(|| {
                        state
                            .official_accounts
                            .iter()
                            .position(|saved| official_account_identity_matches(saved, &incoming))
                    });
                if let Some(existing_index) = existing_index {
                    let existing = &state.official_accounts[existing_index];
                    incoming.id = existing.id.clone();
                    incoming.created_at = existing.created_at;
                    // Remarks have a dedicated update path. Credential refreshes and
                    // repeated logins must never overwrite a concurrent user edit.
                    incoming.remark = existing.remark.clone();
                    // Credential refreshes and repeated device logins must not clear
                    // the last quota snapshot. Quota has its own dedicated update path.
                    incoming.quota = existing.quota.clone();
                    incoming.updated_at = now;
                    let retained_id = incoming.id.clone();
                    let duplicate_accounts = state
                        .official_accounts
                        .iter()
                        .filter(|saved| {
                            saved.id != retained_id
                                && official_account_identity_matches(saved, &incoming)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    // 已有历史重复项也可能拥有另一份备注、额度或维护状态。保存新凭据时，
                    // 保留本次传入的凭据，同时以同一套合并规则补全其他字段。
                    for duplicate in &duplicate_accounts {
                        incoming = merge_legacy_official_account(&incoming, duplicate);
                    }
                    state.official_accounts[existing_index] = incoming.clone();
                    let duplicate_ids = duplicate_accounts
                        .iter()
                        .map(|saved| saved.id.clone())
                        .collect::<Vec<_>>();
                    if matches!(state.active.kind, ActiveKind::Official)
                        && state
                            .active
                            .account_id
                            .as_ref()
                            .is_some_and(|id| duplicate_ids.contains(id))
                    {
                        state.active.account_id = Some(retained_id.clone());
                    }
                    state.official_accounts.retain(|saved| {
                        saved.id == retained_id
                            || !official_account_identity_matches(saved, &incoming)
                    });
                    // 维护状态由本地 id 关联；把重复项的时间线合并到保留 id，
                    // 既不留下悬挂配置，也不因为去重抹去有效的检查/成功记录。
                    for duplicate_id in duplicate_ids {
                        if let Some(duplicate_refresh) =
                            state.credential_refresh.remove(&duplicate_id)
                        {
                            let merged_refresh = state
                                .credential_refresh
                                .get(&retained_id)
                                .map(|saved| {
                                    merge_credential_refresh_state(saved, &duplicate_refresh)
                                })
                                .unwrap_or(duplicate_refresh);
                            state
                                .credential_refresh
                                .insert(retained_id.clone(), merged_refresh);
                        }
                    }
                    saved_accounts.push(incoming);
                    continue;
                }

                if incoming.id.trim().is_empty()
                    || state
                        .official_accounts
                        .iter()
                        .any(|saved| saved.id == incoming.id)
                {
                    incoming.id = uuid::Uuid::new_v4().to_string();
                }
                if incoming.created_at == 0 {
                    incoming.created_at = now;
                }
                incoming.updated_at = now;
                state.official_accounts.push(incoming.clone());
                saved_accounts.push(incoming);
            }
            Ok(saved_accounts)
        })
    }

    #[cfg(test)]
    pub(crate) fn restore_official_accounts_if_matches(
        &self,
        expected: &[StoredOfficialAccount],
        previous: &[StoredOfficialAccount],
    ) -> Result<bool, AppError> {
        self.update(|state| {
            if state.official_accounts != expected {
                return Ok(false);
            }
            state.official_accounts = previous.to_vec();
            Ok(true)
        })
    }

    pub fn ensure_official_account_capacity(
        &self,
        accounts: &[StoredOfficialAccount],
    ) -> Result<(), AppError> {
        let mut incoming_accounts = accounts.to_vec();
        for account in &mut incoming_accounts {
            normalize_official_account(account)?;
        }
        self.read(|state| {
            ensure_official_account_capacity(&state.official_accounts, &incoming_accounts)
        })?
    }

    pub fn update_official_account_remark(
        &self,
        id: &str,
        remark: String,
    ) -> Result<StoredOfficialAccount, AppError> {
        let mut saved = self.update_official_account_remarks(vec![AccountRemarkUpdate {
            id: id.to_owned(),
            remark,
        }])?;
        saved
            .pop()
            .ok_or_else(|| AppError::Internal("更新 OpenAI 账号备注后未返回账号数据。".into()))
    }

    pub fn update_official_account_remarks(
        &self,
        updates: Vec<AccountRemarkUpdate>,
    ) -> Result<Vec<StoredOfficialAccount>, AppError> {
        self.update(|state| {
            let account_indices = state
                .official_accounts
                .iter()
                .enumerate()
                .map(|(index, account)| (account.id.clone(), index))
                .collect::<BTreeMap<_, _>>();
            let mut seen = BTreeSet::new();
            let mut normalized = Vec::with_capacity(updates.len());

            // 在修改 draft 前验证整批输入，任意一条失败都不会产生部分更新。
            for update in updates {
                let remark = update.remark.trim().to_owned();
                ensure_char_limit(
                    &remark,
                    MAX_ACCOUNT_REMARK_CHARS,
                    "账号备注不能超过 200 个字符。",
                )?;
                if !seen.insert(update.id.clone()) {
                    return Err(AppError::InvalidConfig(
                        "同一个 OpenAI 账号不能在一批修改中重复出现。".into(),
                    ));
                }
                let index = account_indices.get(&update.id).copied().ok_or_else(|| {
                    AppError::InvalidConfig("OpenAI 账号不存在，可能已被删除。".into())
                })?;
                normalized.push((index, remark));
            }

            let now = chrono::Utc::now().timestamp();
            let mut saved = Vec::with_capacity(normalized.len());
            for (index, remark) in normalized {
                let account = &mut state.official_accounts[index];
                account.remark = remark;
                account.updated_at = now;
                saved.push(account.clone());
            }
            Ok(saved)
        })
    }

    pub fn save_official_account_quota(
        &self,
        id: &str,
        quota: ProviderAccountQuota,
    ) -> Result<ProviderAccountQuota, AppError> {
        self.update(|state| {
            let account = state
                .official_accounts
                .iter_mut()
                .find(|account| account.id == id)
                .ok_or_else(|| {
                    AppError::InvalidConfig("OpenAI 账号不存在，可能已被删除。".into())
                })?;
            account.quota = quota.clone();
            Ok(quota)
        })
    }

    /// 详情请求只更新重置卡摘要，不能用一个较旧的完整额度快照覆盖其它字段。
    pub fn save_official_account_reset_credit_summary(
        &self,
        id: &str,
        summary: ResetCreditSummary,
    ) -> Result<(), AppError> {
        self.update(|state| {
            let account = state
                .official_accounts
                .iter_mut()
                .find(|account| account.id == id)
                .ok_or_else(|| {
                    AppError::InvalidConfig("OpenAI 账号不存在，可能已被删除。".into())
                })?;
            account.quota.reset_credits = summary;
            Ok(())
        })
    }

    /// 仅在某个窗口成功通过完整性门禁后更新该窗口；其它窗口和旧成功结论保持不变。
    pub fn save_official_account_quota_estimates(
        &self,
        id: &str,
        estimates: &[QuotaEstimate],
    ) -> Result<(), AppError> {
        self.update(|state| {
            let account = state
                .official_accounts
                .iter_mut()
                .find(|account| account.id == id)
                .ok_or_else(|| {
                    AppError::InvalidConfig("OpenAI 账号不存在，可能已被删除。".into())
                })?;
            for estimate in estimates {
                account.quota.estimates.retain(|saved| {
                    saved.window_seconds != estimate.window_seconds
                        || saved.reset_at != estimate.reset_at
                });
                account.quota.estimates.push(estimate.clone());
            }
            Ok(())
        })
    }

    /// 新一轮估算开始时，先撤销这些当前窗口的旧金额；失败时绝不保留可展示的旧结论。
    pub fn clear_official_account_quota_estimates(
        &self,
        id: &str,
        windows: &[(i64, i64)],
    ) -> Result<(), AppError> {
        if windows.is_empty() {
            return Ok(());
        }
        self.update(|state| {
            let account = state
                .official_accounts
                .iter_mut()
                .find(|account| account.id == id)
                .ok_or_else(|| {
                    AppError::InvalidConfig("OpenAI 账号不存在，可能已被删除。".into())
                })?;
            account.quota.estimates.retain(|estimate| {
                !windows.iter().any(|(window_seconds, reset_at)| {
                    estimate.window_seconds == *window_seconds && estimate.reset_at == *reset_at
                })
            });
            Ok(())
        })
    }

    pub fn sync_official_credential(
        &self,
        id: &str,
        credential: &CodexAuthCredential,
        expires_at: Option<i64>,
    ) -> Result<StoredOfficialAccount, AppError> {
        validate_official_credential(credential)?;
        self.update(|state| {
            let account = state
                .official_accounts
                .iter_mut()
                .find(|account| account.id == id)
                .ok_or_else(|| AppError::InvalidConfig("OpenAI 账号不存在，请重新登录。".into()))?;
            if !official_credential_identity_matches(account, credential) {
                return Err(AppError::InvalidConfig(
                    "OpenAI 返回了其他账号的登录信息，请重新登录。".into(),
                ));
            }
            if account.credential == *credential && account.expires_at == expires_at {
                return Ok(account.clone());
            }
            account.credential = credential.clone();
            account.expires_at = expires_at;
            account.updated_at = chrono::Utc::now().timestamp();
            state.credential_refresh.remove(id);
            Ok(account.clone())
        })
    }

    pub fn connections_activate_official_account(&self, id: &str) -> Result<(), AppError> {
        self.update(|state| {
            if !state
                .official_accounts
                .iter()
                .any(|account| account.id == id)
            {
                return Err(AppError::InvalidConfig(
                    "OpenAI 账号不存在，请重新登录。".into(),
                ));
            }
            state.active = ActiveState {
                kind: ActiveKind::Official,
                provider_id: None,
                account_id: Some(id.to_owned()),
            };
            state.codex.last_managed_model = None;
            Ok(())
        })
    }

    pub fn delete_official_account(&self, id: &str) -> Result<(), AppError> {
        self.delete_official_accounts(vec![id.to_owned()])
    }

    pub fn delete_official_accounts(&self, ids: Vec<String>) -> Result<(), AppError> {
        self.update(|state| {
            let ids = ids.into_iter().collect::<BTreeSet<_>>();
            let deleted_accounts = state
                .official_accounts
                .iter()
                .filter(|account| ids.contains(&account.id))
                .cloned()
                .collect::<Vec<_>>();
            let deleted_identity_ids = deleted_accounts
                .iter()
                .map(canonical_official_account_id)
                .collect::<BTreeSet<_>>();
            if matches!(state.active.kind, ActiveKind::Official)
                && state
                    .active
                    .account_id
                    .as_ref()
                    .and_then(|id| {
                        state
                            .official_accounts
                            .iter()
                            .find(|account| &account.id == id)
                    })
                    .is_some_and(|account| {
                        deleted_identity_ids.contains(&canonical_official_account_id(account))
                    })
            {
                return Err(AppError::InvalidConfig(
                    "正在使用这个 OpenAI 账号，请先切换后再删除。".into(),
                ));
            }

            // 先确认所有账号都存在，再一次性修改 draft。
            let existing = state
                .official_accounts
                .iter()
                .map(|account| account.id.as_str())
                .collect::<BTreeSet<_>>();
            if ids.iter().any(|id| !existing.contains(id.as_str())) {
                return Err(AppError::InvalidConfig(
                    "OpenAI 账号不存在，可能已被删除。".into(),
                ));
            }

            let removed_ids = state
                .official_accounts
                .iter()
                .filter(|account| {
                    deleted_identity_ids.contains(&canonical_official_account_id(account))
                })
                .map(|account| account.id.clone())
                .collect::<BTreeSet<_>>();
            state.official_accounts.retain(|account| {
                !deleted_identity_ids.contains(&canonical_official_account_id(account))
            });
            state
                .credential_refresh
                .retain(|id, _| !removed_ids.contains(id));
            state
                .deletion_tombstones
                .official_account_ids
                .extend(deleted_identity_ids);
            Ok(())
        })
    }
}

fn ensure_official_account_capacity(
    existing: &[StoredOfficialAccount],
    incoming: &[StoredOfficialAccount],
) -> Result<(), AppError> {
    let mut staged = existing.to_vec();
    for account in incoming {
        if !staged
            .iter()
            .any(|saved| official_account_identity_matches(saved, account))
        {
            staged.push(account.clone());
        }
    }
    if staged.len() > MAX_SAVED_OPENAI_ACCOUNTS {
        return Err(AppError::InvalidConfig(
            "最多可保存 500 个 OpenAI 账号，请先删除不再使用的账号。".into(),
        ));
    }
    Ok(())
}

fn normalize_official_account(account: &mut StoredOfficialAccount) -> Result<(), AppError> {
    account.id = account.id.trim().to_owned();
    account.name = account.name.trim().to_owned();
    account.remark = account.remark.trim().to_owned();
    account.account_id = account.account_id.trim().to_owned();
    account.email = account.email.trim().to_owned();
    if account.name.is_empty() {
        account.name = if account.email.is_empty() {
            "OpenAI".into()
        } else {
            account.email.clone()
        };
    }
    if account.account_id.is_empty() {
        return Err(AppError::InvalidConfig(
            "OpenAI 登录信息缺少账号标识，请重新登录。".into(),
        ));
    }
    ensure_char_limit(
        &account.name,
        MAX_DISPLAY_NAME_CHARS,
        "账号名称不能超过 100 个字符。",
    )?;
    ensure_char_limit(
        &account.remark,
        MAX_ACCOUNT_REMARK_CHARS,
        "账号备注不能超过 200 个字符。",
    )?;
    ensure_char_limit(
        &account.account_id,
        MAX_ACCOUNT_ID_CHARS,
        "OpenAI 账号标识不能超过 512 个字符。",
    )?;
    ensure_char_limit(
        &account.email,
        MAX_EMAIL_CHARS,
        "账号邮箱不能超过 320 个字符。",
    )?;
    validate_official_credential(&account.credential)?;
    if account.account_id != account.credential.tokens.account_id {
        return Err(AppError::InvalidConfig(
            "OpenAI 账号与登录信息不匹配，请重新登录。".into(),
        ));
    }
    Ok(())
}

fn preserve_redacted_headers(
    incoming: &mut std::collections::BTreeMap<String, String>,
    existing: &std::collections::BTreeMap<String, String>,
) {
    for (name, value) in incoming.iter_mut() {
        if value.is_empty()
            && let Some(saved) = existing.get(name)
        {
            *value = saved.clone();
        }
    }
}

fn persist_files(
    root: &Path,
    app_path: &Path,
    connections_path: &Path,
    credentials_path: &Path,
    tombstones_path: &Path,
    state: &AppConfig,
) -> anyhow::Result<()> {
    let result = persist_files_with_hook(
        root,
        app_path,
        connections_path,
        credentials_path,
        tombstones_path,
        state,
        |_| Ok(()),
    );
    if let Err(commit_error) = result {
        if root.join(STATE_TRANSACTION_FILE).exists() {
            return recover_state_transaction(root).map_err(|recovery_error| {
                anyhow::anyhow!(
                    "状态事务提交失败且无法恢复：{commit_error}（恢复错误：{recovery_error}）"
                )
            });
        }
        return Err(commit_error);
    }
    Ok(())
}

fn persist_files_with_hook(
    root: &Path,
    app_path: &Path,
    connections_path: &Path,
    credentials_path: &Path,
    tombstones_path: &Path,
    state: &AppConfig,
    mut after_file: impl FnMut(usize) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    ensure_state_paths(
        root,
        app_path,
        connections_path,
        credentials_path,
        tombstones_path,
    )?;
    let completed = root.join(COMPLETED_STATE_TRANSACTION_FILE);
    if completed.exists() {
        fs::remove_file(&completed)?;
        sync_parent_directory(&completed)?;
    }
    let app = AppFile {
        codex: state.codex.clone(),
        active: state.active.clone(),
    };
    let mut providers = Vec::with_capacity(state.providers.len());
    let mut credentials = CredentialsFile::default();
    for provider in &state.providers {
        let mut public = provider.clone();
        if let Some(key) = public.api_key.take() {
            credentials
                .provider_api_keys
                .insert(provider.id.clone(), key);
        }
        if !public.headers.is_empty() {
            credentials
                .provider_headers
                .insert(provider.id.clone(), public.headers.clone());
            public.headers = public
                .headers
                .keys()
                .map(|name| (name.clone(), String::new()))
                .collect();
        }
        providers.push(public);
    }
    let connections = ConnectionsFile {
        providers,
        official_accounts: state.official_accounts.clone(),
        credential_refresh: state.credential_refresh.clone(),
    };
    let transaction = StateTransaction {
        version: STATE_TRANSACTION_VERSION,
        app,
        connections,
        credentials,
        deletion_tombstones: state.deletion_tombstones.clone(),
    };
    JsonStore::write_atomic(&root.join(STATE_TRANSACTION_FILE), &transaction)?;
    after_file(0)?;
    apply_state_transaction(root, &transaction, &mut after_file)?;
    clear_state_transaction(root, &mut after_file)?;
    Ok(())
}

fn ensure_state_paths(
    root: &Path,
    app_path: &Path,
    connections_path: &Path,
    credentials_path: &Path,
    tombstones_path: &Path,
) -> anyhow::Result<()> {
    for (actual, expected) in [
        (app_path, root.join("app.json")),
        (connections_path, root.join("connections.json")),
        (credentials_path, root.join("credentials.json")),
        (tombstones_path, root.join("deletion_tombstones.json")),
    ] {
        if actual != expected {
            anyhow::bail!("状态文件路径不在应用数据目录中：{}", actual.display());
        }
    }
    Ok(())
}

fn apply_state_transaction(
    root: &Path,
    transaction: &StateTransaction,
    after_file: &mut impl FnMut(usize) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    if transaction.version != STATE_TRANSACTION_VERSION {
        anyhow::bail!("不支持的状态事务版本：{}", transaction.version);
    }
    JsonStore::write_atomic(&root.join("app.json"), &transaction.app)?;
    after_file(1)?;
    JsonStore::write_atomic(&root.join("connections.json"), &transaction.connections)?;
    after_file(2)?;
    JsonStore::write_atomic(&root.join("credentials.json"), &transaction.credentials)?;
    after_file(3)?;
    JsonStore::write_atomic(
        &root.join("deletion_tombstones.json"),
        &transaction.deletion_tombstones,
    )?;
    after_file(4)?;
    Ok(())
}

fn recover_state_transaction(root: &Path) -> anyhow::Result<()> {
    let completed = root.join(COMPLETED_STATE_TRANSACTION_FILE);
    if completed.exists() && fs::remove_file(&completed).is_ok() {
        let _ = sync_parent_directory(&completed);
    }
    let journal = root.join(STATE_TRANSACTION_FILE);
    if !journal.exists() {
        return Ok(());
    }
    if fs::metadata(&journal)?.len() > MAX_STATE_TRANSACTION_BYTES {
        anyhow::bail!("状态事务文件超过 129 MB，拒绝恢复");
    }
    let transaction = serde_json::from_slice::<StateTransaction>(&fs::read(&journal)?)
        .map_err(|error| anyhow::anyhow!("状态事务文件损坏：{error}"))?;
    apply_state_transaction(root, &transaction, &mut |_| Ok(()))?;
    clear_state_transaction(root, &mut |_| Ok(()))
}

fn clear_state_transaction(
    root: &Path,
    after_file: &mut impl FnMut(usize) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let journal = root.join(STATE_TRANSACTION_FILE);
    if !journal.exists() {
        return Ok(());
    }
    let completed = root.join(COMPLETED_STATE_TRANSACTION_FILE);
    if completed.exists() {
        fs::remove_file(&completed)?;
    }
    replace_file(&journal, &completed)?;
    after_file(5)?;
    // Renaming the journal is the commit point. Cleanup failures after this
    // leave only an idempotent completion marker and must not stale memory.
    let _ = sync_parent_directory(&completed);
    if fs::remove_file(&completed).is_ok() {
        let _ = sync_parent_directory(&completed);
    }
    after_file(6)?;
    Ok(())
}

fn state_files_complete(
    app_path: &Path,
    connections_path: &Path,
    credentials_path: &Path,
    tombstones_path: &Path,
) -> bool {
    app_path.exists()
        && connections_path.exists()
        && credentials_path.exists()
        && tombstones_path.exists()
}

fn load_state_files(root: &Path) -> anyhow::Result<AppConfig> {
    let app_file = JsonStore::read_or_default(&root.join("app.json"), AppFile::default)?;
    let connections =
        JsonStore::read_or_default(&root.join("connections.json"), ConnectionsFile::default)?;
    let credentials =
        JsonStore::read_or_default(&root.join("credentials.json"), CredentialsFile::default)?;
    let mut providers = connections.providers;
    for provider in &mut providers {
        if let Some(key) = credentials.provider_api_keys.get(&provider.id) {
            provider.api_key = Some(key.clone());
            provider.has_api_key = true;
        }
        if let Some(headers) = credentials.provider_headers.get(&provider.id) {
            provider.headers = headers.clone();
        }
    }
    Ok(AppConfig {
        codex: app_file.codex,
        active: app_file.active,
        providers,
        official_accounts: connections.official_accounts,
        credential_refresh: connections.credential_refresh,
        deletion_tombstones: load_deletion_tombstones(&root.join("deletion_tombstones.json"))?,
    })
}

fn load_deletion_tombstones(path: &Path) -> anyhow::Result<DeletionTombstones> {
    JsonStore::read_or_default(path, DeletionTombstones::default)
}

/// 删除事实只保存本地 provider id 或官方账号的不可逆 canonical id。加载和
/// 旧版重写后都以它为准；同时清理被删除记录留下的维护状态与悬空 active 引用。
fn remove_tombstoned_connections(state: &mut AppConfig) -> bool {
    let mut changed = false;
    let provider_count = state.providers.len();
    state.providers.retain(|provider| {
        !state
            .deletion_tombstones
            .provider_ids
            .contains(&provider.id)
    });
    changed |= state.providers.len() != provider_count;

    let mut removed_account_ids = BTreeSet::new();
    state.official_accounts.retain(|account| {
        let deleted = state
            .deletion_tombstones
            .official_account_ids
            .contains(&canonical_official_account_id(account));
        if deleted {
            removed_account_ids.insert(account.id.clone());
        }
        !deleted
    });
    if !removed_account_ids.is_empty() {
        changed = true;
        state
            .credential_refresh
            .retain(|id, _| !removed_account_ids.contains(id));
    }

    let dangling_active = match state.active.kind {
        ActiveKind::Provider => !state
            .active
            .provider_id
            .as_ref()
            .is_some_and(|id| state.providers.iter().any(|provider| &provider.id == id)),
        ActiveKind::Official => !state.active.account_id.as_ref().is_some_and(|id| {
            state
                .official_accounts
                .iter()
                .any(|account| &account.id == id)
        }),
        ActiveKind::None => false,
    };
    if dangling_active {
        state.active = ActiveState::default();
        changed = true;
    }
    changed
}

fn merge_current_duplicate_official_accounts(state: &mut AppConfig) -> bool {
    let mut merged_accounts = Vec::with_capacity(state.official_accounts.len());
    let mut retained_ids = BTreeMap::new();
    let active_account_id = matches!(state.active.kind, ActiveKind::Official)
        .then_some(state.active.account_id.as_deref())
        .flatten();

    for account in std::mem::take(&mut state.official_accounts) {
        if let Some(index) = merged_accounts
            .iter()
            .position(|saved: &StoredOfficialAccount| {
                official_account_identity_matches(saved, &account)
            })
        {
            let active_duplicate = active_account_id == Some(account.id.as_str());
            let existing = merged_accounts[index].clone();
            let replacement = if active_duplicate {
                merge_legacy_official_account(&account, &existing)
            } else {
                merge_legacy_official_account(&existing, &account)
            };
            let previous = std::mem::replace(&mut merged_accounts[index], replacement);
            let retained_id = merged_accounts[index].id.clone();
            retained_ids.insert(previous.id, retained_id.clone());
            retained_ids.insert(account.id, retained_id);
        } else {
            merged_accounts.push(account);
        }
    }
    state.official_accounts = merged_accounts;

    for (duplicate_id, retained_id) in &retained_ids {
        if duplicate_id == retained_id {
            continue;
        }
        if let Some(duplicate_refresh) = state.credential_refresh.remove(duplicate_id) {
            let merged_refresh = state
                .credential_refresh
                .get(retained_id)
                .map(|saved| merge_credential_refresh_state(saved, &duplicate_refresh))
                .unwrap_or(duplicate_refresh);
            state
                .credential_refresh
                .insert(retained_id.clone(), merged_refresh);
        }
    }
    if matches!(state.active.kind, ActiveKind::Official) {
        if let Some(retained_id) = state
            .active
            .account_id
            .as_ref()
            .and_then(|id| retained_ids.get(id))
        {
            state.active.account_id = Some(retained_id.clone());
        }
    }

    !retained_ids.is_empty()
}

fn merge_legacy_official_account(
    current: &StoredOfficialAccount,
    legacy: &StoredOfficialAccount,
) -> StoredOfficialAccount {
    let legacy_is_newer = account_updated_at(legacy) > account_updated_at(current);
    let (newer, older) = if legacy_is_newer {
        (legacy, current)
    } else {
        (current, legacy)
    };
    let mut merged = newer.clone();
    // 当前目录的本地 id 是稳定的 UI / active 引用，始终保留它。
    merged.id = current.id.clone();
    // 登录/刷新不会有意清空这些展示字段；两侧有值时保留更新较新的一个。
    merged.name = preferred_nonempty(&newer.name, &older.name);
    merged.email = preferred_nonempty(&newer.email, &older.email);
    merged.account_id = preferred_nonempty(&newer.account_id, &older.account_id);
    merged.remark = merge_account_remark(&current.remark, &legacy.remark, legacy_is_newer);
    merged.quota = merge_account_quota(&current.quota, &legacy.quota);
    merged.created_at = earliest_positive_timestamp(current.created_at, legacy.created_at);
    merged.updated_at = current.updated_at.max(legacy.updated_at);
    merged
}

fn account_updated_at(account: &StoredOfficialAccount) -> i64 {
    if account.updated_at > 0 {
        account.updated_at
    } else {
        account.expires_at.unwrap_or_default()
    }
}

fn preferred_nonempty(preferred: &str, fallback: &str) -> String {
    if preferred.trim().is_empty() {
        fallback.to_owned()
    } else {
        preferred.to_owned()
    }
}

fn merge_account_remark(current: &str, legacy: &str, legacy_is_newer: bool) -> String {
    let current = current.trim();
    let legacy = legacy.trim();
    if current.is_empty() || current == legacy {
        return legacy.to_owned();
    }
    if legacy.is_empty() {
        return current.to_owned();
    }
    if current.lines().any(|line| line == legacy) {
        return current.to_owned();
    }
    let combined = format!("{current}\n{legacy}");
    if combined.chars().count() <= MAX_ACCOUNT_REMARK_CHARS {
        combined
    } else if legacy_is_newer {
        legacy.to_owned()
    } else {
        current.to_owned()
    }
}

fn merge_account_quota(
    current: &ProviderAccountQuota,
    legacy: &ProviderAccountQuota,
) -> ProviderAccountQuota {
    let legacy_is_newer = quota_updated_at(legacy) > quota_updated_at(current)
        || (quota_updated_at(legacy) == quota_updated_at(current)
            && !quota_is_meaningful(current)
            && quota_is_meaningful(legacy));
    let (newer, older) = if legacy_is_newer {
        (legacy, current)
    } else {
        (current, legacy)
    };
    let mut merged = newer.clone();
    if merged.data.is_none() {
        merged.data = older.data.clone();
    }
    if merged.plan_type.is_none() {
        merged.plan_type = older.plan_type.clone();
    }
    if merged.error.is_none() {
        merged.error = older.error.clone();
    }
    if merged.error_code.is_none() {
        merged.error_code = older.error_code.clone();
    }
    if merged.reset_credits.available_count.is_none() {
        merged.reset_credits = older.reset_credits.clone();
    }
    merged.fetched_at = max_optional_timestamp(current.fetched_at, legacy.fetched_at);
    merged.last_attempt_at =
        max_optional_timestamp(current.last_attempt_at, legacy.last_attempt_at);
    for estimate in &older.estimates {
        if let Some(saved) = merged.estimates.iter_mut().find(|saved| {
            saved.window_seconds == estimate.window_seconds && saved.reset_at == estimate.reset_at
        }) {
            if estimate.estimated_at > saved.estimated_at {
                *saved = estimate.clone();
            }
        } else {
            merged.estimates.push(estimate.clone());
        }
    }
    merged
}

fn discard_outdated_quota_estimates(state: &mut AppConfig) -> bool {
    let mut changed = false;
    for account in &mut state.official_accounts {
        let estimate_count = account.quota.estimates.len();
        account.quota.estimates.retain(|estimate| {
            estimate.calculation_version == CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION
        });
        changed |= account.quota.estimates.len() != estimate_count;
    }
    changed
}

fn quota_updated_at(quota: &ProviderAccountQuota) -> i64 {
    quota
        .fetched_at
        .into_iter()
        .chain(quota.last_attempt_at)
        .max()
        .unwrap_or_default()
}

fn quota_is_meaningful(quota: &ProviderAccountQuota) -> bool {
    !matches!(quota.status, QuotaStatus::Never)
        || quota.data.is_some()
        || quota.plan_type.is_some()
        || quota.fetched_at.is_some()
        || quota.last_attempt_at.is_some()
        || quota.error.is_some()
        || quota.error_code.is_some()
        || !quota.estimates.is_empty()
        || quota.reset_credits.available_count.is_some()
}

fn max_optional_timestamp(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn earliest_positive_timestamp(left: i64, right: i64) -> i64 {
    match (left, right) {
        (0, value) | (value, 0) => value,
        (left, right) => left.min(right),
    }
}

fn merge_credential_refresh_state(
    current: &CredentialRefreshState,
    legacy: &CredentialRefreshState,
) -> CredentialRefreshState {
    let legacy_is_newer =
        credential_refresh_updated_at(legacy) > credential_refresh_updated_at(current);
    let newer = if legacy_is_newer { legacy } else { current };
    CredentialRefreshState {
        status: newer.status,
        last_attempt_at: max_optional_timestamp(current.last_attempt_at, legacy.last_attempt_at),
        last_success_at: max_optional_timestamp(current.last_success_at, legacy.last_success_at),
        next_retry_at: max_optional_timestamp(current.next_retry_at, legacy.next_retry_at),
        retry_count: current.retry_count.max(legacy.retry_count),
        last_refresh_at: max_optional_timestamp(current.last_refresh_at, legacy.last_refresh_at),
        last_sync_at: max_optional_timestamp(current.last_sync_at, legacy.last_sync_at),
        last_check_at: max_optional_timestamp(current.last_check_at, legacy.last_check_at),
        verification: newer.verification,
    }
}

fn credential_refresh_updated_at(refresh: &CredentialRefreshState) -> i64 {
    [
        refresh.last_attempt_at,
        refresh.last_success_at,
        refresh.last_refresh_at,
        refresh.last_sync_at,
        refresh.last_check_at,
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or_default()
}

pub fn data_root() -> PathBuf {
    if let Some(value) = std::env::var_os("CODEX_TOOLS_DATA_DIR") {
        return PathBuf::from(value);
    }
    #[cfg(target_os = "macos")]
    {
        dirs::data_dir()
            .or_else(|| dirs::home_dir().map(|home| home.join("Library/Application Support")))
            .unwrap_or_else(|| PathBuf::from("."))
            .join("io.github.irasutoya.codex-tools")
    }
    #[cfg(not(target_os = "macos"))]
    {
        // 保持旧版安装布局：数据库位于可执行文件同级的 data/ 目录。
        // 覆盖安装只替换程序文件，仍读取原目录，因此无需迁移或复制数据。
        executable_data_root(std::env::current_exe().ok().as_deref())
    }
}

#[cfg(not(target_os = "macos"))]
fn executable_data_root(executable: Option<&Path>) -> PathBuf {
    executable
        .and_then(Path::parent)
        .map(|directory| directory.join("data"))
        .unwrap_or_else(|| PathBuf::from(".").join("data"))
}

fn mark_active_profile(state: &AppConfig, profile: &mut ProviderProfile) {
    profile.active = matches!(state.active.kind, ActiveKind::Provider)
        && state.active.provider_id.as_deref() == Some(profile.id.as_str());
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if fs::read(path).is_ok_and(|current| current == bytes) {
        secure_file(path)?;
        return Ok(());
    }
    let temporary = write_temporary(path, bytes)?;
    replace_temporary(&temporary, path)
}

pub fn atomic_write_if_unchanged(
    path: &Path,
    expected: &[u8],
    bytes: &[u8],
) -> anyhow::Result<bool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if expected == bytes {
        return match fs::read(path) {
            Ok(current) if current == expected => {
                secure_file(path)?;
                Ok(true)
            }
            Ok(_) | Err(_) => Ok(false),
        };
    }
    let temporary = write_temporary(path, bytes)?;
    let unchanged = fs::read(path).is_ok_and(|current| current == expected);
    if !unchanged {
        let _ = fs::remove_file(&temporary);
        return Ok(false);
    }
    replace_temporary(&temporary, path)?;
    Ok(true)
}

fn write_temporary(path: &Path, bytes: &[u8]) -> anyhow::Result<PathBuf> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("data");
    let temporary = path.with_file_name(format!(".{name}.{}.tmp", uuid::Uuid::new_v4().simple()));
    let mut file = create_private_file(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    Ok(temporary)
}

fn replace_temporary(temporary: &Path, path: &Path) -> anyhow::Result<()> {
    if let Err(error) = replace_file(temporary, path) {
        let _ = fs::remove_file(temporary);
        return Err(error);
    }
    sync_parent_directory(path)?;
    Ok(())
}

fn create_private_file(path: &Path) -> std::io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(unix)]
pub(crate) fn secure_directory(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
pub(crate) fn secure_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
pub(crate) fn secure_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
pub(crate) fn secure_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn replace_file(source: &Path, target: &Path) -> anyhow::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows::{
        Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        },
        core::PCWSTR,
    };
    let source = source
        .as_os_str()
        .encode_wide()
        .chain([0])
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain([0])
        .collect::<Vec<_>>();
    unsafe {
        MoveFileExW(
            PCWSTR(source.as_ptr()),
            PCWSTR(target.as_ptr()),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )?;
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_file(source: &Path, target: &Path) -> anyhow::Result<()> {
    fs::rename(source, target)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

    #[test]
    fn rejects_oversized_app_data_before_parsing() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("app.json");
        let file = fs::File::create(&path).unwrap();
        file.set_len(MAX_APP_DATA_BYTES + 1).unwrap();

        let error = Store::open(temp.path().to_path_buf()).err().unwrap();

        assert!(error.to_string().contains("超过 32 MB"));
    }

    #[test]
    fn rejects_oversized_state_transaction_before_parsing() {
        let temp = tempfile::tempdir().unwrap();
        let journal = temp.path().join(STATE_TRANSACTION_FILE);
        let file = fs::File::create(journal).unwrap();
        file.set_len(MAX_STATE_TRANSACTION_BYTES + 1).unwrap();

        let error = Store::open(temp.path().to_path_buf()).err().unwrap();

        assert!(error.to_string().contains("状态事务文件超过 129 MB"));
        assert!(!temp.path().join("app.json").exists());
    }

    fn official_account(account_id: &str, suffix: &str) -> StoredOfficialAccount {
        StoredOfficialAccount {
            id: String::new(),
            name: format!("OpenAI {suffix}"),
            remark: String::new(),
            account_id: account_id.into(),
            email: format!("{suffix}@example.test"),
            credential: CodexAuthCredential {
                auth_mode: "chatgpt".into(),
                openai_api_key: None,
                tokens: CodexAuthTokens {
                    id_token: format!("id-secret-{suffix}"),
                    access_token: format!("access-secret-{suffix}"),
                    refresh_token: format!("refresh-secret-{suffix}"),
                    account_id: account_id.into(),
                },
                last_refresh: "2026-07-14T00:00:00Z".into(),
            },
            source: OfficialAccountSource::OpenAiOauth,
            expires_at: Some(1_800_000_000),
            quota: ProviderAccountQuota::default(),
            created_at: 0,
            updated_at: 0,
        }
    }

    fn identified_official_account(
        account_id: &str,
        subject: &str,
        email: &str,
        suffix: &str,
    ) -> StoredOfficialAccount {
        let mut account = official_account(account_id, suffix);
        account.email = email.into();
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::json!({
                "sub": subject,
                "chatgpt_account_id": account_id,
                "email": email
            })
            .to_string()
            .as_bytes(),
        );
        account.credential.tokens.id_token = format!("header.{payload}.signature");
        account
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn executable_data_root_uses_installed_app_sibling_data_directory() {
        let root = executable_data_root(Some(Path::new(
            "C:/Users/mihai/AppData/Local/Codex Tools/codex-tools.exe",
        )));

        assert_eq!(
            root,
            PathBuf::from("C:/Users/mihai/AppData/Local/Codex Tools/data")
        );
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn executable_data_root_falls_back_to_relative_data_directory() {
        assert_eq!(executable_data_root(None), PathBuf::from(".").join("data"));
    }

    #[test]
    fn reopening_current_state_deduplicates_without_a_legacy_directory() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        store
            .update(|state| {
                let mut first = identified_official_account(
                    "shared-workspace",
                    "same-person",
                    "person@example.test",
                    "first",
                );
                first.id = "stale-record".into();
                first.remark = "旧备注".into();
                first.created_at = 10;
                first.updated_at = 10;
                first.quota = ProviderAccountQuota {
                    status: QuotaStatus::Success,
                    fetched_at: Some(10),
                    estimates: vec![QuotaEstimate {
                        window_seconds: 18_000,
                        reset_at: 100,
                        estimated_total_microusd: 100,
                        estimated_at: 10,
                        calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
                    }],
                    ..Default::default()
                };
                let mut active = first.clone();
                active.id = "active-record".into();
                active.credential.tokens.access_token = "active-newer-credential".into();
                active.remark = "活动备注".into();
                active.updated_at = 20;
                active.quota = ProviderAccountQuota {
                    status: QuotaStatus::Success,
                    fetched_at: Some(20),
                    estimates: vec![QuotaEstimate {
                        window_seconds: 604_800,
                        reset_at: 200,
                        estimated_total_microusd: 200,
                        estimated_at: 20,
                        calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
                    }],
                    ..Default::default()
                };
                state.providers.push(provider("unrelated-provider"));
                state.official_accounts = vec![first, active];
                state.active = ActiveState {
                    kind: ActiveKind::Official,
                    provider_id: None,
                    account_id: Some("active-record".into()),
                };
                state.credential_refresh.insert(
                    "stale-record".into(),
                    CredentialRefreshState {
                        last_success_at: Some(30),
                        ..Default::default()
                    },
                );
                state.credential_refresh.insert(
                    "active-record".into(),
                    CredentialRefreshState {
                        last_check_at: Some(40),
                        ..Default::default()
                    },
                );
                Ok(())
            })
            .unwrap();
        drop(store);

        let reopened = Store::open(root.clone()).unwrap();
        let state = reopened.snapshot().unwrap();
        assert_eq!(state.providers.len(), 1);
        assert_eq!(state.official_accounts.len(), 1);
        assert_eq!(state.active.account_id.as_deref(), Some("active-record"));
        let merged = &state.official_accounts[0];
        assert_eq!(merged.id, "active-record");
        assert_eq!(
            merged.credential.tokens.access_token,
            "active-newer-credential"
        );
        assert_eq!(merged.remark, "活动备注\n旧备注");
        assert_eq!(merged.created_at, 10);
        assert_eq!(merged.updated_at, 20);
        assert_eq!(merged.quota.fetched_at, Some(20));
        assert_eq!(merged.quota.estimates.len(), 2);
        assert_eq!(
            state
                .credential_refresh
                .get("active-record")
                .and_then(|refresh| refresh.last_success_at),
            Some(30)
        );
        assert_eq!(
            state
                .credential_refresh
                .get("active-record")
                .and_then(|refresh| refresh.last_check_at),
            Some(40)
        );
        assert!(!state.credential_refresh.contains_key("stale-record"));
        let once_reopened = serde_json::to_value(&state).unwrap();
        drop(reopened);

        let reopened_again = Store::open(root).unwrap();
        assert_eq!(
            serde_json::to_value(reopened_again.snapshot().unwrap()).unwrap(),
            once_reopened
        );
    }

    fn provider(id: &str) -> ProviderProfile {
        ProviderProfile {
            id: id.into(),
            name: id.into(),
            base_url: "https://example.test/v1".into(),
            headers: Default::default(),
            timeout_secs: 30,
            enabled: true,
            active: false,
            model: String::new(),

            model_context_windows: Default::default(),
            context_window_override: None,
            available_models: Default::default(),
            selected_models: None,
            custom_models: Default::default(),
            models_dev_meta: Default::default(),
            api_type: ProviderApiType::Responses,
            api_key: Some("secret".into()),
            has_api_key: false,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn provider_deletion_tombstone_survives_old_connections_rewrite() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let mut retained = provider("retained");
        retained
            .headers
            .insert("x-retained".into(), "preserve".into());
        store.connections_save_provider(retained).unwrap();
        store
            .connections_save_provider(provider("deleted"))
            .unwrap();
        store.connections_save_provider(provider("other")).unwrap();
        store.connections_delete_provider("deleted").unwrap();

        fs::write(
            root.join("connections.json"),
            serde_json::to_vec(&ConnectionsFile {
                providers: vec![provider("retained"), provider("deleted"), provider("other")],
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();

        let reopened = Store::open(root.clone()).unwrap();
        let state = reopened.snapshot().unwrap();
        assert_eq!(
            state
                .providers
                .iter()
                .map(|provider| provider.id.as_str())
                .collect::<Vec<_>>(),
            vec!["retained", "other"]
        );
        assert_eq!(
            reopened
                .provider("retained")
                .unwrap()
                .headers
                .get("x-retained"),
            Some(&"preserve".to_owned())
        );
        assert!(root.join("deletion_tombstones.json").exists());

        reopened
            .connections_save_provider(provider("deleted"))
            .unwrap();
        assert!(
            !reopened
                .snapshot()
                .unwrap()
                .deletion_tombstones
                .provider_ids
                .contains("deleted")
        );
        drop(reopened);
        let recovered = Store::open(root).unwrap();
        assert_eq!(recovered.snapshot().unwrap().providers.len(), 3);
    }

    #[test]
    fn official_deletion_tombstone_blocks_legacy_identity_and_allows_relogin() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let deleted = store
            .save_official_account(&identified_official_account(
                "shared-workspace",
                "deleted-person",
                "person@example.test",
                "deleted",
            ))
            .unwrap();
        let retained = store
            .save_official_account(&identified_official_account(
                "shared-workspace",
                "other-person",
                "person@example.test",
                "retained",
            ))
            .unwrap();
        store.delete_official_account(&deleted.id).unwrap();

        let mut revived = identified_official_account(
            "shared-workspace",
            "deleted-person",
            "person@example.test",
            "legacy-record",
        );
        revived.id = "legacy-record".into();
        fs::write(
            root.join("connections.json"),
            serde_json::to_vec(&ConnectionsFile {
                official_accounts: vec![revived, retained.clone()],
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();
        fs::write(
            root.join("app.json"),
            serde_json::json!({
                "codex": {},
                "active": { "kind": "official", "accountId": "legacy-record" }
            })
            .to_string(),
        )
        .unwrap();

        let reopened = Store::open(root.clone()).unwrap();
        let state = reopened.snapshot().unwrap();
        assert_eq!(state.official_accounts.len(), 1);
        assert_eq!(state.official_accounts[0].id, retained.id);
        assert!(matches!(state.active.kind, ActiveKind::None));
        let tombstones = fs::read_to_string(root.join("deletion_tombstones.json")).unwrap();
        assert!(!tombstones.contains("person@example.test"));
        assert!(!tombstones.contains("deleted-person"));

        let restored = reopened
            .save_official_account(&identified_official_account(
                "shared-workspace",
                "deleted-person",
                "person@example.test",
                "relogin",
            ))
            .unwrap();
        assert_eq!(
            reopened.official_account(&restored.id).unwrap().account_id,
            "shared-workspace"
        );
        assert!(
            !reopened
                .snapshot()
                .unwrap()
                .deletion_tombstones
                .official_account_ids
                .contains(&canonical_official_account_id(&restored))
        );
        drop(reopened);

        let once_reopened = Store::open(root.clone()).unwrap();
        let once_state = serde_json::to_value(once_reopened.snapshot().unwrap()).unwrap();
        drop(once_reopened);
        let twice_reopened = Store::open(root).unwrap();
        assert_eq!(
            serde_json::to_value(twice_reopened.snapshot().unwrap()).unwrap(),
            once_state
        );
    }

    #[test]
    fn old_data_without_tombstones_remains_compatible() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        fs::write(
            root.join("app.json"),
            r#"{"codex":{},"active":{"kind":"none"}}"#,
        )
        .unwrap();
        fs::write(
            root.join("connections.json"),
            serde_json::to_vec(&ConnectionsFile {
                providers: vec![provider("legacy-provider")],
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();
        fs::write(root.join("credentials.json"), "{}").unwrap();

        let store = Store::open(root.to_path_buf()).unwrap();
        assert_eq!(store.snapshot().unwrap().providers[0].id, "legacy-provider");
        assert!(root.join("deletion_tombstones.json").exists());
    }

    #[test]
    fn creates_app_json_without_touching_unrelated_files() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("unrelated.yaml"), "invalid: [").unwrap();
        fs::write(temp.path().join("unrelated.txt"), "user data").unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        assert!(store.path().ends_with("app.json"));
        assert_eq!(
            fs::read_to_string(temp.path().join("unrelated.txt")).unwrap(),
            "user data"
        );
    }

    #[test]
    fn opening_legacy_app_data_drops_deleted_device_session_settings() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("app.json"),
            r#"{
  "codex": {},
  "active": { "kind": "none" },
  "officialInstallationIdSettings": {
    "legacy": { "enabled": true, "installationId": "old", "sessionId": "old" }
  }
}"#,
        )
        .unwrap();

        Store::open(temp.path().to_path_buf()).unwrap();

        let persisted = fs::read_to_string(temp.path().join("app.json")).unwrap();
        assert!(!persisted.contains("officialInstallationIdSettings"));
        assert!(!persisted.contains("installationId"));
        assert!(!persisted.contains("sessionId"));
    }

    #[cfg(unix)]
    #[test]
    fn app_data_permissions_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");
        let store = Store::open(root.clone()).unwrap();
        assert_eq!(
            fs::metadata(root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(store.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn json_updates_are_atomic_and_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        store
            .update(|state| {
                state.codex.home = "/tmp/round-trip".into();
                Ok(())
            })
            .unwrap();
        let reopened = Store::open(temp.path().to_path_buf()).unwrap();
        assert_eq!(reopened.snapshot().unwrap().codex.home, "/tmp/round-trip");
        assert!(fs::read_dir(temp.path()).unwrap().count() >= 7);
    }

    #[test]
    fn conditional_atomic_write_rejects_concurrent_changes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rollout.jsonl");
        fs::write(&path, b"before").unwrap();
        fs::write(&path, b"concurrent append").unwrap();

        let written = atomic_write_if_unchanged(&path, b"before", b"replacement").unwrap();

        assert!(!written);
        assert_eq!(fs::read(path).unwrap(), b"concurrent append");
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn identical_atomic_write_keeps_the_existing_file() {
        use std::os::unix::fs::MetadataExt;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("data.json");
        fs::write(&path, b"unchanged").unwrap();
        let inode = fs::metadata(&path).unwrap().ino();

        atomic_write(&path, b"unchanged").unwrap();

        assert_eq!(fs::metadata(path).unwrap().ino(), inode);
    }

    #[test]
    fn official_account_save_updates_only_the_same_workspace_user() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let first = store
            .save_official_account(&identified_official_account(
                "workspace-1",
                "user-1",
                "first@example.test",
                "first",
            ))
            .unwrap();
        store
            .update(|state| {
                state.official_accounts[0].remark = "保留的备注".into();
                state.official_accounts[0].quota.status = QuotaStatus::Success;
                state.official_accounts[0].quota.fetched_at = Some(42);
                let mut duplicate = state.official_accounts[0].clone();
                duplicate.id = "duplicate-record".into();
                state.official_accounts.push(duplicate);
                Ok(())
            })
            .unwrap();
        let second = store
            .save_official_account(&identified_official_account(
                "workspace-1",
                "user-1",
                "renamed@example.test",
                "second",
            ))
            .unwrap();

        assert_eq!(second.id, first.id);
        assert_eq!(second.created_at, first.created_at);
        assert_eq!(second.name, "OpenAI second");
        assert_eq!(second.remark, "保留的备注");
        assert_eq!(second.quota.status, QuotaStatus::Success);
        assert_eq!(second.quota.fetched_at, Some(42));
        assert_eq!(store.snapshot().unwrap().official_accounts.len(), 1);
        assert!(
            fs::read_to_string(&store.connections_path)
                .unwrap()
                .contains("access-secret-second")
        );

        let mut explicitly_annotated =
            identified_official_account("workspace-1", "user-1", "third@example.test", "third");
        explicitly_annotated.remark = "新备注".into();
        explicitly_annotated.quota = ProviderAccountQuota::default();
        let third = store.save_official_account(&explicitly_annotated).unwrap();
        assert_eq!(third.remark, "保留的备注");
        assert_eq!(third.quota.status, QuotaStatus::Success);
        assert_eq!(third.quota.fetched_at, Some(42));
    }

    #[test]
    fn duplicate_merge_preserves_the_active_record() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let original = store
            .save_official_account(&identified_official_account(
                "workspace-1",
                "user-1",
                "person@example.test",
                "original",
            ))
            .unwrap();
        let active_id = "active-duplicate".to_owned();
        store
            .update(|state| {
                state.official_accounts[0].remark = "主记录备注".into();
                state.official_accounts[0].quota = ProviderAccountQuota {
                    status: QuotaStatus::Success,
                    fetched_at: Some(10),
                    estimates: vec![QuotaEstimate {
                        window_seconds: 18_000,
                        reset_at: 100,
                        estimated_total_microusd: 100,
                        estimated_at: 10,
                        calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
                    }],
                    ..Default::default()
                };
                let mut duplicate = state.official_accounts[0].clone();
                duplicate.id = active_id.clone();
                duplicate.remark = "活动重复项备注".into();
                duplicate.quota = ProviderAccountQuota {
                    status: QuotaStatus::Success,
                    fetched_at: Some(20),
                    estimates: vec![QuotaEstimate {
                        window_seconds: 604_800,
                        reset_at: 200,
                        estimated_total_microusd: 200,
                        estimated_at: 20,
                        calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
                    }],
                    ..Default::default()
                };
                state.official_accounts.push(duplicate);
                state.credential_refresh.insert(
                    original.id.clone(),
                    CredentialRefreshState {
                        last_success_at: Some(30),
                        ..Default::default()
                    },
                );
                state.credential_refresh.insert(
                    active_id.clone(),
                    CredentialRefreshState {
                        last_check_at: Some(40),
                        ..Default::default()
                    },
                );
                state.active = ActiveState {
                    kind: ActiveKind::Official,
                    provider_id: None,
                    account_id: Some(active_id.clone()),
                };
                Ok(())
            })
            .unwrap();

        let saved = store
            .save_official_account(&identified_official_account(
                "workspace-1",
                "user-1",
                "renamed@example.test",
                "updated",
            ))
            .unwrap();
        let state = store.snapshot().unwrap();

        assert_eq!(saved.id, active_id);
        assert_eq!(state.active.account_id.as_deref(), Some(active_id.as_str()));
        assert_eq!(state.official_accounts.len(), 1);
        let merged = &state.official_accounts[0];
        assert_eq!(merged.remark, "活动重复项备注\n主记录备注");
        assert_eq!(merged.quota.fetched_at, Some(20));
        assert_eq!(merged.quota.estimates.len(), 2);
        assert_eq!(
            state
                .credential_refresh
                .get(&active_id)
                .and_then(|refresh| refresh.last_success_at),
            Some(30)
        );
        assert_eq!(
            state
                .credential_refresh
                .get(&active_id)
                .and_then(|refresh| refresh.last_check_at),
            Some(40)
        );
        assert!(!state.credential_refresh.contains_key(&original.id));
        assert!(
            !state
                .official_accounts
                .iter()
                .any(|account| account.id == original.id)
        );
    }

    #[test]
    fn official_account_save_keeps_different_users_in_the_same_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let first = store
            .save_official_account(&identified_official_account(
                "shared-workspace",
                "user-1",
                "first@example.test",
                "first",
            ))
            .unwrap();
        store
            .connections_activate_official_account(&first.id)
            .unwrap();
        let second = store
            .save_official_account(&identified_official_account(
                "shared-workspace",
                "user-2",
                "first@example.test",
                "second",
            ))
            .unwrap();

        assert_ne!(second.id, first.id);
        let state = store.snapshot().unwrap();
        assert_eq!(state.official_accounts.len(), 2);
        assert_eq!(state.active.account_id.as_deref(), Some(first.id.as_str()));
    }

    #[test]
    fn official_account_identity_falls_back_to_case_insensitive_email() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut first = official_account("shared-workspace", "first");
        first.email = "Person@Example.Test".into();
        let first = store.save_official_account(&first).unwrap();
        let mut repeated = official_account("shared-workspace", "repeated");
        repeated.email = "person@example.test".into();
        let repeated = store.save_official_account(&repeated).unwrap();
        let different = store
            .save_official_account(&official_account("shared-workspace", "different"))
            .unwrap();

        assert_eq!(repeated.id, first.id);
        assert_ne!(different.id, first.id);
        assert_eq!(store.snapshot().unwrap().official_accounts.len(), 2);
    }

    #[test]
    fn unidentified_import_does_not_overwrite_identified_workspace_users() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let identified = store
            .save_official_account(&identified_official_account(
                "shared-workspace",
                "user-1",
                "person@example.test",
                "identified",
            ))
            .unwrap();
        let mut unidentified = official_account("shared-workspace", "cookie");
        unidentified.email.clear();
        unidentified.credential.tokens.id_token.clear();
        unidentified.credential.tokens.access_token = "opaque-cookie-access".into();
        let first_import = store.save_official_account(&unidentified).unwrap();
        unidentified.credential.tokens.access_token = "opaque-cookie-updated".into();
        let repeated_import = store.save_official_account(&unidentified).unwrap();

        assert_ne!(first_import.id, identified.id);
        assert_eq!(repeated_import.id, first_import.id);
        assert_eq!(store.snapshot().unwrap().official_accounts.len(), 2);
        assert_eq!(
            store
                .official_account(&identified.id)
                .unwrap()
                .credential
                .tokens
                .access_token,
            "access-secret-identified"
        );
    }

    #[test]
    fn credential_refresh_preserves_the_latest_remark_and_quota() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let saved = store
            .save_official_account(&official_account("workspace-1", "old"))
            .unwrap();
        let mut stale_refresh_result = saved.clone();
        stale_refresh_result.credential.tokens.access_token = "access-refreshed".into();

        store
            .update_official_account_remark(&saved.id, "保留此备注".into())
            .unwrap();
        let quota = ProviderAccountQuota {
            status: QuotaStatus::Success,
            fetched_at: Some(42),
            ..Default::default()
        };
        store.save_official_account_quota(&saved.id, quota).unwrap();

        let refreshed = store
            .save_refreshed_official_account(&saved.id, &stale_refresh_result)
            .unwrap();

        assert_eq!(refreshed.id, saved.id);
        assert_eq!(refreshed.remark, "保留此备注");
        assert_eq!(refreshed.quota.status, QuotaStatus::Success);
        assert_eq!(refreshed.quota.fetched_at, Some(42));
        assert_eq!(refreshed.credential.tokens.access_token, "access-refreshed");
        drop(store);
        let persisted = Store::open(root)
            .unwrap()
            .official_account(&saved.id)
            .unwrap();
        assert_eq!(persisted.remark, "保留此备注");
        assert_eq!(persisted.quota.status, QuotaStatus::Success);
    }

    #[test]
    fn quota_estimates_replace_only_successful_matching_windows() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let account = store
            .save_official_account(&official_account("workspace-1", "estimate"))
            .unwrap();
        let five_hours = QuotaEstimate {
            window_seconds: 18_000,
            reset_at: 100,
            estimated_total_microusd: 100,
            estimated_at: 1,
            calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
        };
        let seven_days = QuotaEstimate {
            window_seconds: 604_800,
            reset_at: 200,
            estimated_total_microusd: 200,
            estimated_at: 1,
            calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
        };
        store
            .save_official_account_quota_estimates(
                &account.id,
                &[five_hours.clone(), seven_days.clone()],
            )
            .unwrap();
        let updated_five_hours = QuotaEstimate {
            estimated_total_microusd: 150,
            estimated_at: 2,
            ..five_hours
        };
        // 只传入成功的 5H；未传入的 7D（例如本轮门禁失败）必须保留。
        store
            .save_official_account_quota_estimates(
                &account.id,
                std::slice::from_ref(&updated_five_hours),
            )
            .unwrap();

        let estimates = store.official_account(&account.id).unwrap().quota.estimates;
        assert_eq!(estimates.len(), 2);
        assert!(estimates.contains(&updated_five_hours));
        assert!(estimates.contains(&seven_days));

        store
            .clear_official_account_quota_estimates(
                &account.id,
                &[(
                    updated_five_hours.window_seconds,
                    updated_five_hours.reset_at,
                )],
            )
            .unwrap();
        let remaining = store.official_account(&account.id).unwrap().quota.estimates;
        assert_eq!(remaining, vec![seven_days]);
    }

    #[test]
    fn reopening_discards_old_quota_estimates_but_keeps_current_calculation_version() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let account = store
            .save_official_account(&official_account("workspace-current", "calculation"))
            .unwrap();
        store
            .save_official_account_quota_estimates(
                &account.id,
                &[
                    QuotaEstimate {
                        window_seconds: 18_000,
                        reset_at: 100,
                        estimated_total_microusd: 100,
                        estimated_at: 1,
                        calculation_version: 0,
                    },
                    QuotaEstimate {
                        window_seconds: 604_800,
                        reset_at: 200,
                        estimated_total_microusd: 200,
                        estimated_at: 1,
                        calculation_version: CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION,
                    },
                ],
            )
            .unwrap();
        drop(store);

        let reopened = Store::open(root.clone()).unwrap();
        let estimates = reopened
            .official_account(&account.id)
            .unwrap()
            .quota
            .estimates;
        assert_eq!(estimates.len(), 1);
        assert_eq!(
            estimates[0].calculation_version,
            CURRENT_QUOTA_ESTIMATE_CALCULATION_VERSION
        );
        let persisted = fs::read_to_string(root.join("connections.json")).unwrap();
        assert!(!persisted.contains("\"calculationVersion\":0"));
    }

    #[test]
    fn credential_refresh_rejects_a_different_account_identity() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let saved = store
            .save_official_account(&official_account("workspace-1", "old"))
            .unwrap();
        store
            .update_official_account_remark(&saved.id, "不能丢失".into())
            .unwrap();
        let mut other_account = saved.clone();
        other_account.account_id = "workspace-2".into();
        other_account.credential.tokens.account_id = "workspace-2".into();

        assert!(
            store
                .save_refreshed_official_account(&saved.id, &other_account)
                .is_err()
        );
        let persisted = store.official_account(&saved.id).unwrap();
        assert_eq!(persisted.account_id, "workspace-1");
        assert_eq!(persisted.remark, "不能丢失");
    }

    #[test]
    fn batch_official_account_save_is_atomic_at_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        for index in 0..MAX_SAVED_OPENAI_ACCOUNTS {
            store
                .save_official_account(&official_account(
                    &format!("workspace-{index}"),
                    &format!("account-{index}"),
                ))
                .unwrap();
        }

        let result = store.save_official_accounts(&[
            official_account("workspace-new-1", "new-1"),
            official_account("workspace-new-2", "new-2"),
        ]);

        assert!(result.is_err());
        assert_eq!(store.snapshot().unwrap().official_accounts.len(), 500);
        assert!(
            !store
                .snapshot()
                .unwrap()
                .official_accounts
                .iter()
                .any(|account| account.account_id == "workspace-new-1")
        );
    }

    #[test]
    fn official_account_restore_requires_the_exact_saved_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let previous = store
            .save_official_account(&official_account("workspace-1", "previous"))
            .unwrap();
        let saved = store
            .save_official_account(&official_account("workspace-1", "replacement"))
            .unwrap();
        store
            .update_official_account_remark(&saved.id, "concurrent".into())
            .unwrap();

        assert!(
            !store
                .restore_official_accounts_if_matches(
                    std::slice::from_ref(&saved),
                    std::slice::from_ref(&previous),
                )
                .unwrap()
        );
        assert_eq!(
            store.official_account(&saved.id).unwrap().remark,
            "concurrent"
        );
    }

    #[test]
    fn official_account_remark_is_trimmed_validated_and_persisted() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let saved = store
            .save_official_account(&official_account("workspace-1", "person"))
            .unwrap();
        store
            .update(|state| {
                state.official_accounts[0].updated_at = 1;
                Ok(())
            })
            .unwrap();

        let updated = store
            .update_official_account_remark(&saved.id, "  工作账号  ".into())
            .unwrap();

        assert_eq!(updated.remark, "工作账号");
        assert!(updated.updated_at > 1);
        assert_eq!(
            store.official_account_view(&saved.id).unwrap().remark,
            "工作账号"
        );
        assert_eq!(
            Store::open(root)
                .unwrap()
                .official_account(&saved.id)
                .unwrap()
                .remark,
            "工作账号"
        );
        assert!(
            store
                .update_official_account_remark(
                    &saved.id,
                    "备".repeat(MAX_ACCOUNT_REMARK_CHARS + 1),
                )
                .is_err()
        );
    }

    #[test]
    fn batch_account_remarks_validate_all_inputs_before_updating() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let first = store
            .save_official_account(&official_account("workspace-1", "first"))
            .unwrap();
        let second = store
            .save_official_account(&official_account("workspace-2", "second"))
            .unwrap();

        let saved = store
            .update_official_account_remarks(vec![
                AccountRemarkUpdate {
                    id: second.id.clone(),
                    remark: "  第二个  ".into(),
                },
                AccountRemarkUpdate {
                    id: first.id.clone(),
                    remark: "  第一个  ".into(),
                },
            ])
            .unwrap();
        assert_eq!(
            saved
                .iter()
                .map(|account| (account.id.as_str(), account.remark.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (second.id.as_str(), "第二个"),
                (first.id.as_str(), "第一个")
            ]
        );

        for invalid in [
            vec![
                AccountRemarkUpdate {
                    id: first.id.clone(),
                    remark: "不应保存".into(),
                },
                AccountRemarkUpdate {
                    id: "missing".into(),
                    remark: "missing".into(),
                },
            ],
            vec![
                AccountRemarkUpdate {
                    id: first.id.clone(),
                    remark: "重复一".into(),
                },
                AccountRemarkUpdate {
                    id: first.id.clone(),
                    remark: "重复二".into(),
                },
            ],
            vec![
                AccountRemarkUpdate {
                    id: first.id.clone(),
                    remark: "不应保存".into(),
                },
                AccountRemarkUpdate {
                    id: second.id.clone(),
                    remark: "备".repeat(MAX_ACCOUNT_REMARK_CHARS + 1),
                },
            ],
        ] {
            assert!(store.update_official_account_remarks(invalid).is_err());
            assert_eq!(store.official_account(&first.id).unwrap().remark, "第一个");
            assert_eq!(store.official_account(&second.id).unwrap().remark, "第二个");
        }

        let reopened = Store::open(root).unwrap();
        assert_eq!(
            reopened.official_account(&first.id).unwrap().remark,
            "第一个"
        );
        assert_eq!(
            reopened.official_account(&second.id).unwrap().remark,
            "第二个"
        );
    }

    #[test]
    fn batch_account_delete_is_deduplicated_prevalidated_and_atomic() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let first = store
            .save_official_account(&official_account("workspace-1", "first"))
            .unwrap();
        let second = store
            .save_official_account(&official_account("workspace-2", "second"))
            .unwrap();
        let active = store
            .save_official_account(&official_account("workspace-3", "active"))
            .unwrap();
        store
            .connections_activate_official_account(&active.id)
            .unwrap();

        assert!(
            store
                .delete_official_accounts(vec![first.id.clone(), "missing".into()])
                .is_err()
        );
        assert!(store.official_account(&first.id).is_ok());
        assert!(store.official_account(&second.id).is_ok());
        assert!(
            store
                .delete_official_accounts(vec![first.id.clone(), active.id.clone()])
                .is_err()
        );
        assert!(store.official_account(&first.id).is_ok());
        assert!(store.official_account(&active.id).is_ok());

        store
            .delete_official_accounts(vec![first.id.clone(), first.id.clone(), second.id.clone()])
            .unwrap();
        assert!(store.official_account(&first.id).is_err());
        assert!(store.official_account(&second.id).is_err());
        assert!(store.official_account(&active.id).is_ok());

        let reopened = Store::open(root).unwrap();
        assert!(reopened.official_account(&first.id).is_err());
        assert!(reopened.official_account(&second.id).is_err());
        assert!(reopened.official_account(&active.id).is_ok());
    }

    #[test]
    fn official_views_are_redacted_and_active_uses_local_record_id() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let saved = store
            .save_official_account(&official_account("workspace-1", "person"))
            .unwrap();
        store
            .connections_activate_official_account(&saved.id)
            .unwrap();

        let state = store.snapshot().unwrap();
        assert!(matches!(state.active.kind, ActiveKind::Official));
        assert_eq!(state.active.provider_id, None);
        assert_eq!(state.active.account_id.as_deref(), Some(saved.id.as_str()));

        let view = store.official_account_view(&saved.id).unwrap();
        assert!(view.active);
        assert_eq!(view.remark, "");
        let serialized = serde_json::to_string(&view).unwrap();
        assert!(serialized.contains("\"remark\":\"\""));
        assert!(!serialized.contains("\"credential\":"));
        assert!(!serialized.contains("access-secret"));
        assert!(!serialized.contains("refresh-secret"));
        assert!(!serialized.contains("id-secret"));
    }

    #[test]
    fn provider_overview_is_single_snapshot_and_redacts_secrets() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        store
            .update(|state| {
                state.providers.push(ProviderProfile {
                    id: "provider-1".into(),
                    name: "Provider".into(),
                    base_url: "https://example.test/v1".into(),
                    headers: [("x-secret".into(), "provider-secret".into())]
                        .into_iter()
                        .collect(),
                    timeout_secs: 30,
                    enabled: true,
                    active: false,
                    model: String::new(),

                    model_context_windows: Default::default(),
                    context_window_override: None,
                    available_models: Default::default(),
                    selected_models: None,
                    custom_models: Default::default(),
                    models_dev_meta: Default::default(),
                    api_type: ProviderApiType::Responses,
                    api_key: Some("api-secret".into()),
                    has_api_key: false,
                    created_at: 1,
                    updated_at: 1,
                });
                state.active = ActiveState {
                    kind: ActiveKind::Provider,
                    provider_id: Some("provider-1".into()),
                    account_id: None,
                };
                Ok(())
            })
            .unwrap();

        let overview = store.provider_overview().unwrap();
        assert!(overview.providers[0].active);
        assert_eq!(overview.providers[0].headers["x-secret"], "");
        assert!(overview.providers[0].api_key.is_none());
        let serialized = serde_json::to_string(&overview).unwrap();
        assert!(!serialized.contains("provider-secret"));
        assert!(!serialized.contains("api-secret"));
    }

    #[test]
    fn credential_sync_checks_identity_and_active_account_cannot_be_deleted() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let saved = store
            .save_official_account(&official_account("workspace-1", "old"))
            .unwrap();
        let replacement = official_account("workspace-1", "new").credential;
        let updated = store
            .sync_official_credential(&saved.id, &replacement, Some(1_900_000_000))
            .unwrap();
        assert_eq!(updated.credential.tokens.access_token, "access-secret-new");
        assert_eq!(updated.expires_at, Some(1_900_000_000));

        let wrong = official_account("workspace-2", "wrong").credential;
        assert!(
            store
                .sync_official_credential(&saved.id, &wrong, None)
                .is_err()
        );

        store
            .connections_activate_official_account(&saved.id)
            .unwrap();
        assert!(store.delete_official_account(&saved.id).is_err());
    }

    #[test]
    fn credential_updates_reject_a_different_user_in_the_same_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let saved = store
            .save_official_account(&identified_official_account(
                "shared-workspace",
                "user-1",
                "first@example.test",
                "first",
            ))
            .unwrap();
        let other = identified_official_account(
            "shared-workspace",
            "user-2",
            "second@example.test",
            "second",
        );

        assert!(
            store
                .sync_official_credential(&saved.id, &other.credential, other.expires_at)
                .is_err()
        );
        assert!(
            store
                .save_refreshed_official_account(&saved.id, &other)
                .is_err()
        );
        assert_eq!(
            credential_subject(&store.official_account(&saved.id).unwrap().credential).as_deref(),
            Some("user-1")
        );
    }

    #[test]
    fn official_activation_clears_the_managed_provider_model_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let saved = store
            .save_official_account(&official_account("workspace-1", "official"))
            .unwrap();
        store
            .save_last_managed_model(Some("third-party-model".into()))
            .unwrap();

        store
            .connections_activate_official_account(&saved.id)
            .unwrap();

        let state = store.snapshot().unwrap();
        assert_eq!(state.active.account_id.as_deref(), Some(saved.id.as_str()));
        assert_eq!(state.codex.last_managed_model, None);
    }

    #[test]
    fn provider_requires_an_api_key_before_activation() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut without_key = provider("empty");
        without_key.api_key = None;
        store.connections_save_provider(without_key).unwrap();

        assert!(store.activate("empty").is_err());

        let mut with_key = provider("ready");
        with_key.api_key = Some("secret".into());
        store.connections_save_provider(with_key).unwrap();
        store.activate("ready").unwrap();
        assert!(store.provider("ready").unwrap().active);
    }

    #[test]
    fn restore_active_state_returns_to_the_previous_connection() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut first = provider("first");
        first.api_key = Some("secret".into());
        store.connections_save_provider(first).unwrap();
        let mut second = provider("second");
        second.api_key = Some("secret".into());
        store.connections_save_provider(second).unwrap();
        store.activate("first").unwrap();
        let previous = store.read(|state| state.active.clone()).unwrap();
        store.activate("second").unwrap();

        store.restore_active_state(&previous).unwrap();

        let active = store.read(|state| state.active.clone()).unwrap();
        assert!(matches!(active.kind, ActiveKind::Provider));
        assert_eq!(active.provider_id.as_deref(), Some("first"));
        assert!(store.provider("first").unwrap().active);
        assert!(!store.provider("second").unwrap().active);
    }

    #[test]
    fn restore_active_state_rejects_a_missing_previous_connection() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        store.connections_save_provider(provider("active")).unwrap();
        store.activate("active").unwrap();
        let missing = ActiveState {
            kind: ActiveKind::Provider,
            provider_id: Some("deleted".into()),
            account_id: None,
        };

        assert!(store.restore_active_state(&missing).is_err());
        assert!(matches!(
            store.read(|state| state.active.clone()).unwrap().kind,
            ActiveKind::Provider
        ));
    }

    #[test]
    fn restore_active_state_returns_to_a_previous_official_account() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let saved = store
            .save_official_account(&official_account("workspace", "one"))
            .unwrap();
        store
            .connections_activate_official_account(&saved.id)
            .unwrap();
        let previous = store.read(|state| state.active.clone()).unwrap();
        let mut next = provider("next");
        next.api_key = Some("secret".into());
        store.connections_save_provider(next).unwrap();
        store.activate("next").unwrap();

        store.restore_active_state(&previous).unwrap();

        let active = store.read(|state| state.active.clone()).unwrap();
        assert!(matches!(active.kind, ActiveKind::Official));
        assert_eq!(active.account_id.as_deref(), Some(saved.id.as_str()));
    }

    #[test]
    fn active_provider_cannot_be_disabled() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        store.connections_save_provider(provider("active")).unwrap();
        store.activate("active").unwrap();

        let mut disabled = store.provider("active").unwrap();
        disabled.enabled = false;
        assert!(store.connections_save_provider(disabled).is_err());
        assert!(store.provider("active").unwrap().enabled);
    }

    #[test]
    fn editing_redacted_provider_preserves_the_saved_api_key() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        store
            .connections_save_provider(provider("editable"))
            .unwrap();

        // 模拟前端保存：overview 返回的是脱敏后的 profile（api_key 为空），
        // 编辑后只改了名称，密钥必须保留。
        let mut redacted = store.provider_overview().unwrap().providers[0].clone();
        assert!(redacted.api_key.is_none());
        assert!(redacted.has_api_key);
        redacted.name = "改名后的服务".into();
        store.connections_save_provider(redacted).unwrap();

        let saved = store.provider("editable").unwrap();
        assert_eq!(saved.name, "改名后的服务");
        assert_eq!(saved.api_key.as_deref(), Some("secret"));
    }

    #[test]
    fn saving_all_models_clears_previous_selection_and_survives_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let mut initial = provider("selection");
        initial.available_models = vec!["model-a".into(), "model-b".into()];
        initial.selected_models = Some(vec!["model-a".into()]);
        store.connections_save_provider(initial).unwrap();

        // ProviderSaveInput 的 null 会转换为 None，表示明确清除旧筛选。
        let mut edited = store.provider_overview().unwrap().providers[0].clone();
        edited.selected_models = None;
        let saved = store.connections_save_provider(edited).unwrap();

        assert_eq!(saved.selected_models, None);
        drop(store);
        let reopened = Store::open(root).unwrap();
        let persisted = reopened.provider("selection").unwrap();
        assert_eq!(persisted.selected_models, None);
        assert_eq!(persisted.available_models, vec!["model-a", "model-b"]);
    }

    #[test]
    fn saving_a_new_model_subset_replaces_the_previous_selection() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut initial = provider("selection-subset");
        initial.available_models = vec!["model-a".into(), "model-b".into()];
        initial.selected_models = Some(vec!["model-a".into()]);
        store.connections_save_provider(initial).unwrap();

        let mut edited = store.provider_overview().unwrap().providers[0].clone();
        edited.selected_models = Some(vec!["model-b".into()]);
        let saved = store.connections_save_provider(edited).unwrap();

        assert_eq!(saved.selected_models, Some(vec!["model-b".into()]));
    }

    #[test]
    fn saving_provider_discards_legacy_manual_model() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut incoming = provider("legacy-model");
        incoming.model = "manually-entered-model".into();

        let saved = store.connections_save_provider(incoming).unwrap();

        assert!(saved.model.is_empty());
        assert!(store.provider("legacy-model").unwrap().model.is_empty());
    }

    #[test]
    fn changing_model_source_discards_catalog_but_keeps_and_filters_explicit_selection() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut initial = provider("catalog");
        initial.available_models = vec!["old-api-model".into()];
        initial.selected_models = Some(vec!["old-api-model".into()]);
        initial.model_context_windows = BTreeMap::from([("old-api-model".into(), 128_000)]);
        initial.models_dev_meta =
            BTreeMap::from([("old-api-model".into(), ProviderModelsDevMeta::default())]);
        store.connections_save_provider(initial).unwrap();

        let mut edited = store.provider_overview().unwrap().providers[0].clone();
        edited.base_url = "https://new.example.test/v1".into();
        edited.selected_models = Some(vec!["new-api-model".into(), "stale-model".into()]);
        let saved = store.connections_save_provider(edited).unwrap();

        assert!(saved.available_models.is_empty());
        assert!(saved.model_context_windows.is_empty());
        assert!(saved.models_dev_meta.is_empty());
        assert_eq!(
            saved.selected_models,
            Some(vec!["new-api-model".into(), "stale-model".into()])
        );
    }

    #[test]
    fn renaming_provider_preserves_selected_models_until_catalog_refresh() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut initial = provider("rename-selection");
        initial.available_models = vec!["model-a".into(), "model-b".into()];
        initial.selected_models = Some(vec!["model-b".into()]);
        store.connections_save_provider(initial).unwrap();

        let mut edited = store.provider_overview().unwrap().providers[0].clone();
        edited.name = "新的显示名称".into();
        let saved = store.connections_save_provider(edited).unwrap();

        // 名称会影响 models.dev 匹配并触发目录刷新，但不能扩大用户的模型选择。
        assert!(saved.available_models.is_empty());
        assert_eq!(saved.selected_models, Some(vec!["model-b".into()]));
    }

    #[test]
    fn changing_model_source_preserves_custom_models() {
        // 自定义模型属于用户数据：修改服务地址（source 变化）时不清空。
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut initial = provider("custom-src");
        initial.available_models = vec!["old-api-model".into()];
        initial.custom_models = vec!["custom-keep".into()];
        store.connections_save_provider(initial).unwrap();

        let mut edited = store.provider_overview().unwrap().providers[0].clone();
        edited.base_url = "https://new.example.test/v1".into();
        let saved = store.connections_save_provider(edited).unwrap();

        // /models 同步数据被清空，但自定义模型保留。
        assert!(saved.available_models.is_empty());
        assert_eq!(saved.custom_models, vec!["custom-keep"]);
    }

    #[test]
    fn explicitly_removing_all_custom_models_persists_an_empty_list() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut initial = provider("custom-delete");
        initial.available_models = vec!["api-model".into()];
        initial.custom_models = vec!["custom-remove".into()];
        store.connections_save_provider(initial).unwrap();

        let mut edited = store.provider_overview().unwrap().providers[0].clone();
        edited.custom_models.clear();
        let (_, saved) = store
            .connections_save_provider_with_previous(edited, true)
            .unwrap();

        assert!(saved.custom_models.is_empty());
        assert!(
            store
                .provider("custom-delete")
                .unwrap()
                .custom_models
                .is_empty()
        );
    }

    #[test]
    fn refresh_models_preserves_selected_custom_models() {
        // 刷新 /models 时 selected_models 中已选的自定义模型被保留。
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut initial = provider("refresh-custom");
        initial.available_models = vec!["api-model".into()];
        initial.custom_models = vec!["custom-keep".into()];
        initial.selected_models = Some(vec!["api-model".into(), "custom-keep".into()]);
        let saved = store.connections_save_provider(initial).unwrap();
        let source = ProviderSourceFingerprint::from_provider(&saved);
        assert_eq!(
            saved.selected_models,
            Some(vec!["api-model".into(), "custom-keep".into()])
        );
        store
            .update_provider_models_if_source_matches(
                &saved.id,
                &source,
                None,
                vec!["api-model".into()],
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .unwrap()
            .unwrap();

        let current = store.provider(&saved.id).unwrap();
        assert_eq!(current.available_models, vec!["api-model"]);
        assert_eq!(current.custom_models, vec!["custom-keep"]);
        assert_eq!(
            current.selected_models,
            Some(vec!["api-model".into(), "custom-keep".into()])
        );
    }

    #[test]
    fn refresh_models_preserves_context_window_override_and_providers_are_isolated() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut first = provider("context-first");
        first.context_window_override = Some(262_144);
        let first = store.connections_save_provider(first).unwrap();
        let source = ProviderSourceFingerprint::from_provider(&first);
        let mut second = provider("context-second");
        second.context_window_override = Some(65_536);
        store.connections_save_provider(second).unwrap();

        store
            .update_provider_models_if_source_matches(
                &first.id,
                &source,
                None,
                vec!["new-model".into()],
                BTreeMap::from([("new-model".into(), 128_000)]),
                BTreeMap::new(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            store.provider(&first.id).unwrap().context_window_override,
            Some(262_144)
        );
        assert_eq!(
            store
                .provider("context-second")
                .unwrap()
                .context_window_override,
            Some(65_536)
        );

        let mut cleared = store.provider(&first.id).unwrap();
        cleared.context_window_override = None;
        let cleared = store.connections_save_provider(cleared).unwrap();
        assert_eq!(cleared.context_window_override, None);
        drop(store);
        assert_eq!(
            Store::open(temp.path().to_path_buf())
                .unwrap()
                .provider(&first.id)
                .unwrap()
                .context_window_override,
            None
        );
    }

    #[test]
    fn refresh_models_keeps_an_empty_intersection_explicit_instead_of_selecting_all() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut initial = provider("refresh-no-overlap");
        initial.available_models = vec!["old-model".into()];
        initial.selected_models = Some(vec!["old-model".into()]);
        let saved = store.connections_save_provider(initial).unwrap();
        let source = ProviderSourceFingerprint::from_provider(&saved);

        store
            .update_provider_models_if_source_matches(
                &saved.id,
                &source,
                None,
                vec!["new-model".into()],
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .unwrap()
            .unwrap();

        let current = store.provider(&saved.id).unwrap();
        assert_eq!(current.available_models, vec!["new-model"]);
        assert_eq!(current.selected_models, Some(Vec::new()));
    }

    #[test]
    fn stale_model_fetch_cannot_overwrite_a_new_provider_source() {
        let temp = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(Store::open(temp.path().to_path_buf()).unwrap());
        let saved = store.connections_save_provider(provider("cas")).unwrap();
        let stale_source = ProviderSourceFingerprint::from_provider(&saved);
        let release_stale_response = std::sync::Arc::new(std::sync::Barrier::new(2));
        let stale_store = store.clone();
        let stale_release = release_stale_response.clone();
        let stale_update = std::thread::spawn(move || {
            stale_release.wait();
            stale_store
                .update_provider_models_if_source_matches(
                    "cas",
                    &stale_source,
                    None,
                    vec!["stale-model".into()],
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
                .unwrap()
        });

        let mut edited = store.provider_overview().unwrap().providers[0].clone();
        edited.base_url = "https://new.example.test/v1".into();
        store.connections_save_provider(edited).unwrap();
        release_stale_response.wait();
        let updated = stale_update.join().unwrap();

        assert!(updated.is_none());
        let current = store.provider("cas").unwrap();
        assert_eq!(current.base_url, "https://new.example.test/v1");
        assert!(current.available_models.is_empty());
    }

    #[test]
    fn provider_snapshot_restore_is_exact_and_source_guarded() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().to_path_buf()).unwrap();
        let mut initial = provider("rollback");
        initial.available_models = vec!["old-model".into()];
        initial.model_context_windows = BTreeMap::from([("old-model".into(), 128_000)]);
        initial.models_dev_meta = BTreeMap::from([(
            "old-model".into(),
            ProviderModelsDevMeta {
                name: Some("Old model".into()),
                ..ProviderModelsDevMeta::default()
            },
        )]);
        store.connections_save_provider(initial).unwrap();
        let previous = store.read(|state| state.providers[0].clone()).unwrap();

        let mut changed = store.provider_overview().unwrap().providers[0].clone();
        changed.base_url = "https://changed.example.test/v1".into();
        changed.api_key = Some("changed-secret".into());
        let changed = store.connections_save_provider(changed).unwrap();
        let changed_source = ProviderSourceFingerprint::from_provider(&changed);
        assert!(
            store
                .update_provider_models_if_source_matches(
                    &changed.id,
                    &changed_source,
                    None,
                    vec!["new-model".into()],
                    BTreeMap::from([("new-model".into(), 256_000)]),
                    BTreeMap::new(),
                )
                .unwrap()
                .is_some()
        );
        let changed_revision = store
            .provider_snapshot_revision(&changed.id)
            .unwrap()
            .unwrap();

        assert!(
            store
                .restore_provider_snapshot_if_revision_matches(&changed_revision, &previous)
                .unwrap()
        );
        let restored = store.read(|state| state.providers[0].clone()).unwrap();
        assert_eq!(
            serde_json::to_value(restored).unwrap(),
            serde_json::to_value(&previous).unwrap()
        );

        let restored_revision = store
            .provider_snapshot_revision("rollback")
            .unwrap()
            .unwrap();
        let mut later = store.provider_overview().unwrap().providers[0].clone();
        later.timeout_secs = 91;
        store.connections_save_provider(later).unwrap();
        assert!(
            !store
                .restore_provider_snapshot_if_revision_matches(&restored_revision, &previous)
                .unwrap()
        );
        assert_eq!(store.provider("rollback").unwrap().timeout_secs, 91);
    }

    #[test]
    fn update_persists_mutated_state_to_disk_for_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        store
            .connections_save_provider(provider("persisted"))
            .unwrap();
        store.activate("persisted").unwrap();

        let reopened = Store::open(root).unwrap();
        let state = reopened.snapshot().unwrap();
        assert_eq!(state.providers.len(), 1);
        assert_eq!(state.providers[0].id, "persisted");
        assert_eq!(state.active.provider_id.as_deref(), Some("persisted"));
        assert_eq!(state.active.account_id, None);
    }

    #[test]
    fn interrupted_state_commit_reopens_as_the_complete_new_generation() {
        for interrupted_after in 0..=6 {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join(format!("fault-{interrupted_after}"));
            let store = Store::open(root.clone()).unwrap();
            store
                .connections_save_provider(provider("transaction"))
                .unwrap();
            let mut draft = store.snapshot().unwrap();
            draft.codex.home = "/synthetic/new-home".into();
            draft.providers[0].base_url = "https://new.example.test/v1".into();
            draft.providers[0].api_key = Some("new-secret".into());
            draft
                .deletion_tombstones
                .provider_ids
                .insert("previously-deleted".into());

            let error = persist_files_with_hook(
                &root,
                &root.join("app.json"),
                &root.join("connections.json"),
                &root.join("credentials.json"),
                &root.join("deletion_tombstones.json"),
                &draft,
                |completed| {
                    if completed == interrupted_after {
                        anyhow::bail!("injected process interruption");
                    }
                    Ok(())
                },
            )
            .unwrap_err();
            assert!(error.to_string().contains("injected process interruption"));
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                for path in [
                    root.join(STATE_TRANSACTION_FILE),
                    root.join(COMPLETED_STATE_TRANSACTION_FILE),
                ] {
                    if path.exists() {
                        assert_eq!(
                            fs::metadata(path).unwrap().permissions().mode() & 0o777,
                            0o600
                        );
                    }
                }
            }

            let reopened = Store::open(root.clone()).unwrap();
            let provider = reopened.provider("transaction").unwrap();
            assert_eq!(provider.base_url, "https://new.example.test/v1");
            assert_eq!(provider.api_key.as_deref(), Some("new-secret"));
            assert_eq!(
                reopened.snapshot().unwrap().codex.home,
                "/synthetic/new-home"
            );
            assert!(
                reopened
                    .snapshot()
                    .unwrap()
                    .deletion_tombstones
                    .provider_ids
                    .contains("previously-deleted")
            );
            assert!(!root.join("state-transaction.json").exists());
            for name in [
                "app.json",
                "connections.json",
                "credentials.json",
                "deletion_tombstones.json",
            ] {
                let path = root.join(name);
                serde_json::from_slice::<serde_json::Value>(&fs::read(&path).unwrap()).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    assert_eq!(
                        fs::metadata(path).unwrap().permissions().mode() & 0o777,
                        0o600
                    );
                }
            }
        }
    }

    #[test]
    fn failed_persist_leaves_memory_unchanged_and_returns_error() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(temp.path().to_path_buf()).unwrap();
        store.connections_save_provider(provider("one")).unwrap();
        let original_home = store.read(|state| state.codex.home.clone()).unwrap();
        fs::write(temp.path().join("blocked"), b"file").unwrap();
        store.path = temp.path().join("blocked").join("app.json");

        let result = store.update(|state| {
            state.codex.home = "/tmp/alternate".into();
            Ok(())
        });

        assert!(result.is_err());
        assert_eq!(
            store.read(|state| state.codex.home.clone()).unwrap(),
            original_home
        );
    }

    #[test]
    fn stale_completion_marker_cannot_validate_a_later_update() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = Store::open(root.clone()).unwrap();
        let original_home = store.snapshot().unwrap().codex.home;
        fs::create_dir(root.join(COMPLETED_STATE_TRANSACTION_FILE)).unwrap();

        let result = store.update(|state| {
            state.codex.home = "/must-not-commit".into();
            Ok(())
        });

        assert!(result.is_err());
        assert_eq!(store.snapshot().unwrap().codex.home, original_home);
        assert!(!root.join(STATE_TRANSACTION_FILE).exists());
    }

    #[test]
    fn failed_provider_delete_at_credentials_restores_provider_and_api_key() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let mut store = Store::open(root.clone()).unwrap();
        store.connections_save_provider(provider("one")).unwrap();
        fs::write(temp.path().join("blocked"), b"file").unwrap();
        store.credentials_path = temp.path().join("blocked").join("credentials.json");

        assert!(store.connections_delete_provider("one").is_err());
        assert_eq!(
            store.provider("one").unwrap().api_key.as_deref(),
            Some("secret")
        );
        assert_eq!(
            Store::open(root)
                .unwrap()
                .provider("one")
                .unwrap()
                .api_key
                .as_deref(),
            Some("secret")
        );
    }
}
