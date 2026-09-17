use crate::models::SupportNetworkDiagnostics;
use reqwest::{Client, ClientBuilder, NoProxy, Proxy};
use std::{
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

const PROXY_ENV_NAMES: [&str; 8] = [
    "ALL_PROXY",
    "all_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "NO_PROXY",
    "no_proxy",
];
const CODEX_ORIGINATOR: &str = "codex_cli_rs";
const CODEX_LOGIN_SUFFIX: &str = "codex_login";
const FALLBACK_CODEX_VERSION: &str = "0.144.1";

pub(crate) fn codex_user_agent() -> &'static str {
    static USER_AGENT: OnceLock<String> = OnceLock::new();
    USER_AGENT
        .get_or_init(|| {
            build_codex_user_agent(
                detected_codex_version(),
                crate::platform::os_name(),
                &crate::platform::os_version(),
                std::env::consts::ARCH,
            )
        })
        .as_str()
}

fn build_codex_user_agent(version: &str, os_name: &str, os_version: &str, arch: &str) -> String {
    format!(
        "{CODEX_ORIGINATOR}/{version} ({os_name} {os_version}; {arch}) unknown ({CODEX_LOGIN_SUFFIX}; {version})"
    )
}

fn detected_codex_version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| detect_codex_version().unwrap_or_else(|| FALLBACK_CODEX_VERSION.into()))
        .as_str()
}

fn detect_codex_version() -> Option<String> {
    let mut command = crate::platform::codex_cli_command();
    command.arg("--version");
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    parse_codex_version(&String::from_utf8_lossy(&output.stdout))
}

fn parse_codex_version(output: &str) -> Option<String> {
    let mut fields = output.split_whitespace();
    let product = fields.next()?;
    if !matches!(product, "codex-cli" | "codex") {
        return None;
    }
    let version = fields.next()?.trim_start_matches('v');
    if version.is_empty()
        || version.len() > 32
        || !version
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'.' | b'-' | b'+'))
    {
        return None;
    }
    Some(version.to_owned())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SystemProxy {
    http: Option<String>,
    https: Option<String>,
    socks: Option<String>,
    bypass: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProxySnapshot {
    environment: Vec<(String, Option<String>)>,
    platform: Option<SystemProxy>,
}

/// 系统代理快照的缓存时长。读取系统代理在 macOS 上要执行 `scutil` 子进程、
/// 在 Windows 上要读注册表，频繁调用开销不小；代理设置短时间内不会变化，
/// 缓存几秒即可兼顾正确性与开销。
const PROXY_SNAPSHOT_CACHE_TTL: Duration = Duration::from_secs(3);

static SNAPSHOT_CACHE: OnceLock<Mutex<Option<(Instant, ProxySnapshot)>>> = OnceLock::new();

/// 带短 TTL 缓存的系统代理快照：短时间内重复调用不会重复执行子进程/
/// 注册表探测，适合每请求调用的热路径（额度轮询、模型同步、转换代理转发）。
fn cached_proxy_snapshot() -> ProxySnapshot {
    {
        let cache = SNAPSHOT_CACHE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("网络客户端缓存锁已损坏");
        if let Some((fetched_at, snapshot)) = cache.as_ref()
            && fetched_at.elapsed() < PROXY_SNAPSHOT_CACHE_TTL
        {
            return snapshot.clone();
        }
    }
    let snapshot = proxy_snapshot();
    let mut cache = SNAPSHOT_CACHE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("网络客户端缓存锁已损坏");
    if let Some((fetched_at, cached)) = cache.as_ref()
        && fetched_at.elapsed() < PROXY_SNAPSHOT_CACHE_TTL
    {
        return cached.clone();
    }
    *cache = Some((Instant::now(), snapshot.clone()));
    snapshot
}

/// 缓存快照的引用版本：调用方在 TTL 内可以避免克隆整个快照。
/// 返回的 guard 持有 Mutex 锁，调用方应尽快使用后释放。

#[derive(Default)]
pub(crate) struct ClientCache {
    cached: Mutex<Option<CachedClient>>,
}

struct CachedClient {
    snapshot: ProxySnapshot,
    client: Client,
}

impl ClientCache {
    pub(crate) fn current<F>(&self, configure: F) -> Result<Client, reqwest::Error>
    where
        F: FnOnce(ClientBuilder) -> Result<Client, reqwest::Error>,
    {
        self.current_for_snapshot(cached_proxy_snapshot(), configure)
    }

    pub(crate) fn invalidate(&self) {
        self.cached.lock().expect("网络客户端缓存锁已损坏").take();
        *SNAPSHOT_CACHE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("网络客户端缓存锁已损坏") = None;
    }

    /// 带短 TTL 缓存的系统代理快照（关联函数，供转换代理等热路径复用）。
    pub(crate) fn cached_snapshot() -> ProxySnapshot {
        cached_proxy_snapshot()
    }

    /// 按给定系统代理快照构建一个独立客户端（不参与本缓存）。
    /// `timeout` 为 `None` 时不设置整体超时，适合长时间流式请求。
    pub(crate) fn build_standalone(
        snapshot: &ProxySnapshot,
        timeout: Option<Duration>,
    ) -> Result<Client, reqwest::Error> {
        let builder = client_builder_for(snapshot)?
            .connect_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(4)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60));
        match timeout {
            Some(timeout) => builder.timeout(timeout),
            None => builder,
        }
        .build()
    }

    fn current_for_snapshot<F>(
        &self,
        snapshot: ProxySnapshot,
        configure: F,
    ) -> Result<Client, reqwest::Error>
    where
        F: FnOnce(ClientBuilder) -> Result<Client, reqwest::Error>,
    {
        let mut cached = self.cached.lock().expect("网络客户端缓存锁已损坏");
        if cached
            .as_ref()
            .is_some_and(|cached| cached.snapshot == snapshot)
        {
            return Ok(cached
                .as_ref()
                .expect("客户端缓存刚刚检查过")
                .client
                .clone());
        }

        let client = configure(client_builder_for(&snapshot)?)?;
        *cached = Some(CachedClient {
            snapshot,
            client: client.clone(),
        });
        Ok(client)
    }
}

fn proxy_snapshot() -> ProxySnapshot {
    ProxySnapshot {
        environment: PROXY_ENV_NAMES
            .iter()
            .map(|name| ((*name).to_owned(), std::env::var(name).ok()))
            .collect(),
        platform: platform_proxy(),
    }
}

fn client_builder_for(snapshot: &ProxySnapshot) -> Result<ClientBuilder, reqwest::Error> {
    let builder = Client::builder().user_agent(codex_user_agent());
    if environment_proxy_configured(snapshot) {
        return Ok(builder);
    }

    apply_system_proxy(builder, snapshot.platform.as_ref())
}

fn environment_proxy_configured(snapshot: &ProxySnapshot) -> bool {
    snapshot.environment.iter().any(|(name, value)| {
        matches!(
            name.as_str(),
            "ALL_PROXY" | "all_proxy" | "HTTPS_PROXY" | "https_proxy" | "HTTP_PROXY" | "http_proxy"
        ) && value.as_ref().is_some_and(|value| !value.is_empty())
    })
}

pub(crate) fn support_diagnostics() -> SupportNetworkDiagnostics {
    let snapshot = cached_proxy_snapshot();
    let no_proxy_configured = snapshot.environment.iter().any(|(name, value)| {
        matches!(name.as_str(), "NO_PROXY" | "no_proxy")
            && value.as_ref().is_some_and(|value| !value.trim().is_empty())
    });
    SupportNetworkDiagnostics {
        environment_proxy_configured: environment_proxy_configured(&snapshot),
        no_proxy_configured,
        system_proxy_configured: snapshot.platform.is_some_and(|settings| {
            settings.http.is_some() || settings.https.is_some() || settings.socks.is_some()
        }),
        tls_backend: "rustls".into(),
    }
}

fn apply_system_proxy(
    mut builder: ClientBuilder,
    settings: Option<&SystemProxy>,
) -> Result<ClientBuilder, reqwest::Error> {
    let Some(settings) = settings else {
        return Ok(builder);
    };
    let no_proxy = no_proxy(&settings.bypass);

    match (settings.http.as_deref(), settings.https.as_deref()) {
        (Some(http), Some(https)) if http == https => {
            builder = builder.proxy(Proxy::all(http)?.no_proxy(no_proxy.clone()));
        }
        (Some(http), None) => {
            // An HTTP proxy can tunnel HTTPS via CONNECT, so a HTTP-only
            // system setting must cover both URL schemes.
            builder = builder.proxy(Proxy::all(http)?.no_proxy(no_proxy.clone()));
        }
        (None, Some(https)) => {
            builder = builder.proxy(Proxy::https(https)?.no_proxy(no_proxy.clone()));
        }
        (Some(http), Some(https)) => {
            builder = builder.proxy(Proxy::http(http)?.no_proxy(no_proxy.clone()));
            builder = builder.proxy(Proxy::https(https)?.no_proxy(no_proxy.clone()));
        }
        (None, None) => {
            if let Some(socks) = &settings.socks {
                builder = builder.proxy(Proxy::all(socks)?.no_proxy(no_proxy));
            }
        }
    }
    Ok(builder)
}

fn no_proxy(entries: &[String]) -> Option<NoProxy> {
    let entries = entries
        .iter()
        .flat_map(|entry| entry.split([';', ',']))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .flat_map(|entry| {
            if entry.eq_ignore_ascii_case("<local>") {
                vec!["localhost", "127.0.0.1", "::1"]
            } else if entry == "*" {
                vec!["*"]
            } else {
                vec![entry.trim_start_matches('*')]
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    (!entries.is_empty())
        .then(|| NoProxy::from_string(&entries))
        .flatten()
}

fn normalize_proxy_url(value: &str) -> Option<String> {
    let value = value.trim().trim_matches('"');
    let normalized = if value.is_empty() {
        return None;
    } else if value.contains("://") {
        value.to_owned()
    } else {
        format!("http://{value}")
    };
    reqwest::Url::parse(&normalized).ok().map(|_| normalized)
}

#[cfg(any(windows, test))]
fn parse_proxy_server(value: &str) -> SystemProxy {
    let mut settings = SystemProxy::default();
    for part in value
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if let Some((kind, address)) = part.split_once('=') {
            match kind.trim().to_ascii_lowercase().as_str() {
                "http" => settings.http = normalize_proxy_url(address),
                "https" => settings.https = normalize_proxy_url(address),
                "socks" | "socks5" => {
                    settings.socks = normalize_proxy_url(address).map(|url| {
                        if url.starts_with("http://") {
                            url.replacen("http://", "socks5://", 1)
                        } else {
                            url
                        }
                    })
                }
                _ => {}
            }
        } else {
            let proxy = normalize_proxy_url(part);
            settings.http.clone_from(&proxy);
            settings.https = proxy;
        }
    }
    settings
}

#[cfg(windows)]
fn platform_proxy() -> Option<SystemProxy> {
    use winreg::{RegKey, enums::HKEY_CURRENT_USER};

    let internet_settings = RegKey::predef(HKEY_CURRENT_USER)
        .open_subkey("Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings")
        .ok()?;
    let enabled = internet_settings
        .get_value::<u32, _>("ProxyEnable")
        .unwrap_or_default();
    if enabled == 0 {
        return None;
    }

    let server = internet_settings
        .get_value::<String, _>("ProxyServer")
        .ok()?;
    let mut settings = parse_proxy_server(&server);
    settings.bypass = internet_settings
        .get_value::<String, _>("ProxyOverride")
        .ok()
        .into_iter()
        .collect();
    (settings.http.is_some() || settings.https.is_some() || settings.socks.is_some())
        .then_some(settings)
}

#[cfg(target_os = "macos")]
fn platform_proxy() -> Option<SystemProxy> {
    let output = std::process::Command::new("/usr/sbin/scutil")
        .arg("--proxy")
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let text = String::from_utf8(output.stdout).ok()?;
    parse_scutil_proxy(&text)
}

#[cfg(all(not(windows), not(target_os = "macos")))]
fn platform_proxy() -> Option<SystemProxy> {
    None
}

#[cfg(any(test, target_os = "macos"))]
fn parse_scutil_proxy(text: &str) -> Option<SystemProxy> {
    fn value<'a>(text: &'a str, name: &str) -> Option<&'a str> {
        text.lines().find_map(|line| {
            let (key, value) = line.trim().split_once(':')?;
            (key.trim() == name).then(|| value.trim())
        })
    }

    fn endpoint(text: &str, prefix: &str) -> Option<String> {
        if value(text, &format!("{prefix}Enable"))? != "1" {
            return None;
        }
        let host = value(text, &format!("{prefix}Proxy"))?;
        let port = value(text, &format!("{prefix}Port"))?;
        normalize_proxy_url(&format!("{host}:{port}"))
    }

    let mut settings = SystemProxy {
        http: endpoint(text, "HTTP"),
        https: endpoint(text, "HTTPS"),
        socks: endpoint(text, "SOCKS").map(|url| {
            if url.starts_with("http://") {
                url.replacen("http://", "socks5://", 1)
            } else {
                url
            }
        }),
        bypass: Vec::new(),
    };
    let mut in_exceptions = false;
    for line in text.lines().map(str::trim) {
        if line.starts_with("ExceptionsList") {
            in_exceptions = true;
            continue;
        }
        if in_exceptions && line == "}" {
            in_exceptions = false;
            continue;
        }
        if in_exceptions && let Some((_, entry)) = line.split_once(':') {
            let entry = entry.trim();
            if !entry.is_empty() {
                settings.bypass.push(entry.to_owned());
            }
        }
    }
    if value(text, "ExcludeSimpleHostnames") == Some("1") {
        settings.bypass.push("<local>".into());
    }

    (settings.http.is_some() || settings.https.is_some() || settings.socks.is_some())
        .then_some(settings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_agent_matches_codex_cli_shape() {
        assert_eq!(
            build_codex_user_agent("0.144.1", "Windows", "10.0.26100", "x86_64"),
            "codex_cli_rs/0.144.1 (Windows 10.0.26100; x86_64) unknown (codex_login; 0.144.1)"
        );
        assert_eq!(
            build_codex_user_agent("0.144.1", "Mac OS", "15.5", "aarch64"),
            "codex_cli_rs/0.144.1 (Mac OS 15.5; aarch64) unknown (codex_login; 0.144.1)"
        );
        assert_eq!(
            parse_codex_version("codex-cli 0.144.1\r\n"),
            Some("0.144.1".into())
        );
        assert_eq!(parse_codex_version("malicious token value"), None);
    }

    #[test]
    fn parses_windows_shared_and_protocol_proxy_values() {
        let shared = parse_proxy_server("127.0.0.1:7890");
        assert_eq!(shared.http.as_deref(), Some("http://127.0.0.1:7890"));
        assert_eq!(shared.https, shared.http);

        let split = parse_proxy_server("http=proxy.local:8080;https=secure.local:8443;socks=skip");
        assert_eq!(split.http.as_deref(), Some("http://proxy.local:8080"));
        assert_eq!(split.https.as_deref(), Some("http://secure.local:8443"));
        assert_eq!(split.socks.as_deref(), Some("socks5://skip"));
    }

    #[test]
    fn parses_macos_proxy_and_bypass_settings() {
        let settings = parse_scutil_proxy(
            r#"<dictionary> {
  ExceptionsList : <array> {
    0 : *.local
    1 : 10.0.0.0/8
  }
  ExcludeSimpleHostnames : 1
  HTTPEnable : 1
  HTTPPort : 7890
  HTTPProxy : 127.0.0.1
  HTTPSEnable : 1
  HTTPSPort : 7890
  HTTPSProxy : 127.0.0.1
}"#,
        )
        .unwrap();
        assert_eq!(settings.http.as_deref(), Some("http://127.0.0.1:7890"));
        assert_eq!(settings.https, settings.http);
        assert_eq!(settings.socks, None);
        assert_eq!(settings.bypass, vec!["*.local", "10.0.0.0/8", "<local>"]);
    }

    #[test]
    fn parses_macos_socks_only_proxy_settings() {
        let settings = parse_scutil_proxy(
            r#"<dictionary> {
  SOCKSEnable : 1
  SOCKSPort : 7891
  SOCKSProxy : 127.0.0.1
}"#,
        )
        .unwrap();

        assert_eq!(settings.socks.as_deref(), Some("socks5://127.0.0.1:7891"));
    }

    #[test]
    fn builds_clients_from_valid_system_proxy_settings() {
        let settings = SystemProxy {
            http: Some("http://127.0.0.1:7890".into()),
            https: Some("http://127.0.0.1:7890".into()),
            socks: None,
            bypass: vec!["<local>;*.example.com".into()],
        };
        assert!(
            apply_system_proxy(Client::builder(), Some(&settings))
                .and_then(ClientBuilder::build)
                .is_ok()
        );

        let socks = SystemProxy {
            http: None,
            https: None,
            socks: Some("socks5://127.0.0.1:7891".into()),
            bypass: Vec::new(),
        };
        assert!(
            apply_system_proxy(Client::builder(), Some(&socks))
                .and_then(ClientBuilder::build)
                .is_ok()
        );

        let http_only = SystemProxy {
            http: Some("http://127.0.0.1:7890".into()),
            https: None,
            socks: None,
            bypass: Vec::new(),
        };
        assert!(
            apply_system_proxy(Client::builder(), Some(&http_only))
                .and_then(ClientBuilder::build)
                .is_ok()
        );
    }

    #[test]
    fn keeps_bare_wildcard_and_local_hosts_in_no_proxy_list() {
        assert!(no_proxy(&["*".into()]).is_some());
        assert!(no_proxy(&["<local>".into()]).is_some());
        assert!(no_proxy(&["".into()]).is_none());
        assert!(no_proxy(&["  ".into()]).is_none());
    }

    #[test]
    fn rebuilds_cached_client_when_proxy_snapshot_changes() {
        let cache = ClientCache::default();
        let mut builds = 0;
        let direct = ProxySnapshot::default();
        let proxied = ProxySnapshot {
            platform: Some(parse_proxy_server("127.0.0.1:7890")),
            ..direct.clone()
        };

        cache
            .current_for_snapshot(direct.clone(), |_| {
                builds += 1;
                Client::builder().build()
            })
            .unwrap();
        cache
            .current_for_snapshot(direct, |_| {
                builds += 1;
                Client::builder().build()
            })
            .unwrap();
        cache
            .current_for_snapshot(proxied, |_| {
                builds += 1;
                Client::builder().build()
            })
            .unwrap();

        assert_eq!(builds, 2);
    }
}
