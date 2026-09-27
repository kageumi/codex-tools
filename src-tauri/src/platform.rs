use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Command,
};

pub fn codex_cli_command() -> Command {
    Command::new(codex_cli_executable())
}

/// 根据已配置的 Codex Desktop 路径定位其内置 CLI。
///
/// `CODEX_BIN` 始终优先，便于显式指定独立 CLI。配置了桌面程序时不能静默回退
/// 到 PATH：否则自定义安装缺失 `resources/codex.exe` 会退化为难以定位的
/// `program not found`，甚至可能调用另一套 Codex。
pub fn codex_cli_command_for_app(configured_app: Option<&str>) -> Result<Command, String> {
    let configured_app = configured_app.ok_or_else(|| {
        "无法定位 Codex 内置 CLI：未配置 Codex Desktop 路径，历史迁移已停止。".to_owned()
    })?;
    let app_path = validate_codex_app_path(configured_app)
        .map_err(|error| format!("无法定位 Codex 内置 CLI：{error}"))?;
    let executable = codex_cli_from_app_path(&app_path)
        .map(PathBuf::into_os_string)
        .ok_or_else(|| {
            format!(
                "无法定位 Codex 内置 CLI：已配置桌面程序 {}，未找到其 resources 目录中的 CLI。",
                app_path.display()
            )
        })?;
    Ok(Command::new(executable))
}

fn codex_bin_override() -> Option<OsString> {
    match std::env::var_os("CODEX_BIN") {
        Some(configured) if !configured.is_empty() => Some(configured),
        _ => None,
    }
}

fn codex_cli_executable() -> OsString {
    if let Some(configured) = codex_bin_override() {
        return configured;
    }
    let program = if cfg!(windows) { "codex.exe" } else { "codex" };
    if let Some(path) = executable_on_path(program) {
        return path.into_os_string();
    }

    if let Some(path) = codex_app_path(None).and_then(|path| codex_cli_from_app_path(&path)) {
        return path.into_os_string();
    }

    #[cfg(target_os = "macos")]
    if let Some(path) = macos_codex_candidates()
        .into_iter()
        .find(|path| is_executable(path))
    {
        return path.into_os_string();
    }

    program.into()
}

fn codex_cli_from_app_path(app_path: &Path) -> Option<PathBuf> {
    #[cfg(windows)]
    {
        let parent = app_path.parent()?;
        let bundled = parent.join("resources/codex.exe");
        if is_executable(&bundled) {
            return Some(bundled);
        }
        if parent
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.eq_ignore_ascii_case("resources"))
            && app_path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case("codex.exe"))
            && is_executable(app_path)
        {
            return Some(app_path.to_path_buf());
        }
    }
    #[cfg(target_os = "macos")]
    {
        if app_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "codex")
            && app_path
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "Resources")
            && is_executable(app_path)
        {
            return Some(app_path.to_path_buf());
        }
        if app_path.is_dir() {
            let bundled = app_path.join("Contents/Resources/codex");
            if is_executable(&bundled) {
                return Some(bundled);
            }
        } else if let Some(contents) = app_path.parent().and_then(Path::parent) {
            let bundled = contents.join("Resources/codex");
            if is_executable(&bundled) {
                return Some(bundled);
            }
        }
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    if is_executable(app_path) {
        return Some(app_path.to_path_buf());
    }
    None
}

fn executable_on_path(program: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|path| path.join(program))
        .find(|path| is_executable(path))
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(target_os = "macos")]
fn macos_codex_candidates() -> Vec<PathBuf> {
    let mut candidates = vec![
        PathBuf::from("/opt/homebrew/bin/codex"),
        PathBuf::from("/usr/local/bin/codex"),
    ];
    if let Some(home) = dirs::home_dir() {
        candidates.push(home.join(".local/bin/codex"));
        candidates.push(home.join(".npm-global/bin/codex"));
        candidates.push(home.join(".volta/bin/codex"));
        candidates.push(home.join("Library/pnpm/codex"));
        candidates.push(home.join(".bun/bin/codex"));
        if let Ok(versions) = std::fs::read_dir(home.join(".nvm/versions/node")) {
            candidates.extend(
                versions
                    .flatten()
                    .map(|entry| entry.path().join("bin/codex")),
            );
        }
    }
    candidates.extend([
        PathBuf::from("/Applications/Codex.app/Contents/Resources/codex"),
        PathBuf::from("/Applications/ChatGPT.app/Contents/Resources/codex"),
    ]);
    candidates
}

pub fn open_url(url: &str) -> std::io::Result<()> {
    #[cfg(windows)]
    let mut command = {
        let mut command = Command::new("rundll32.exe");
        command.args(["url.dll,FileProtocolHandler", url]);
        command
    };

    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("/usr/bin/open");
        command.arg(url);
        command
    };

    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(url);
        command
    };

    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "系统无法打开链接（退出状态：{status}）"
        )))
    }
}

/// 找到 Codex 桌面应用：优先使用手动配置的路径（.app 目录或可执行文件），
/// 否则自动检测（macOS 返回 `.app` 目录，Windows 返回可执行文件路径）。
pub fn codex_app_path(configured: Option<&str>) -> Option<PathBuf> {
    if let Some(path) = configured.and_then(|path| validate_codex_app_path(path).ok()) {
        return Some(path);
    }
    #[cfg(target_os = "macos")]
    {
        macos_codex_app_path()
    }
    #[cfg(windows)]
    {
        windows_codex_exe()
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        None
    }
}

/// 校验手动配置的应用路径：只接受 macOS `.app`、Unix 可执行文件或 Windows `.exe`。
pub fn validate_codex_app_path(path: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(path.trim());
    if !path.exists() {
        return Err("选择的 ChatGPT / Codex 启动程序不存在。".into());
    }

    if path.is_dir() {
        #[cfg(target_os = "macos")]
        {
            let is_app = path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("app"));
            if is_app && path.join("Contents/MacOS").is_dir() {
                return Ok(path);
            }
            return Err("请选择 ChatGPT / Codex 的 .app 应用，或应用内部的可执行文件。".into());
        }
        #[cfg(not(target_os = "macos"))]
        {
            return Err("请选择 ChatGPT / Codex 的可执行文件，不要选择目录。".into());
        }
    }

    if !path.is_file() {
        return Err("选择的 ChatGPT / Codex 启动程序不是有效文件。".into());
    }

    #[cfg(windows)]
    {
        let is_exe = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"));
        if !is_exe {
            return Err("请选择 ChatGPT.exe 或 Codex.exe。".into());
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = path
            .metadata()
            .map_err(|error| format!("无法读取启动程序权限：{error}"))?
            .permissions()
            .mode();
        if mode & 0o111 == 0 {
            return Err("选择的文件没有执行权限。".into());
        }
    }

    Ok(path)
}

pub fn codex_app_found(configured: Option<&str>) -> bool {
    codex_app_path(configured).is_some()
}

/// Codex 桌面应用或 CLI 当前是否在运行，用于降低认证、配置和会话文件
/// 与 Codex 并发改写的风险。
pub fn codex_app_running(configured: Option<&str>) -> bool {
    #[cfg(target_os = "macos")]
    {
        let app_path = codex_app_path(configured);
        let cli_path = codex_cli_executable();
        if app_path.as_deref().is_some_and(path_running) || path_running(Path::new(&cli_path)) {
            return true;
        }
        ["Codex", "ChatGPT", "codex"]
            .into_iter()
            .any(process_named_running)
    }
    #[cfg(windows)]
    {
        let app_path = codex_app_path(configured);
        let cli_path = codex_cli_executable();
        let mut names: Vec<&str> = Vec::with_capacity(5);
        if let Some(name) = app_path
            .as_deref()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
        {
            names.push(name);
        }
        if let Some(name) = Path::new(&cli_path)
            .file_name()
            .and_then(|name| name.to_str())
        {
            names.push(name);
        }
        names.extend(["Codex.exe", "codex.exe", "ChatGPT.exe"]);
        names.dedup();
        any_process_named_running(names)
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        false
    }
}

/// 以调试模式启动 Codex 桌面应用：传入 `--remote-debugging-port` 和
/// `--remote-allow-origins`，使本应用可以通过 CDP 注入解锁脚本。
/// `configured` 为手动指定的 `.app` 目录或可执行文件路径。
pub fn dashboard_launch_app_with_debug(port: u16, configured: Option<&str>) -> std::io::Result<()> {
    let debug_args = [
        format!("--remote-debugging-port={port}"),
        format!("--remote-allow-origins=http://127.0.0.1:{port}"),
    ];
    #[cfg(target_os = "macos")]
    {
        let path = codex_app_path(configured);
        if let Some(path) = path.as_deref().filter(|path| !path.is_dir()) {
            // 手动指定的可执行文件：直接运行。
            Command::new(path).args(&debug_args).spawn().map(|_| ())
        } else {
            let app = path.as_deref().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "未找到 Codex 桌面应用")
            })?;
            let status = Command::new("/usr/bin/open")
                .arg("-a")
                .arg(app)
                .arg("--args")
                .args(&debug_args)
                .status()?;
            if status.success() {
                Ok(())
            } else {
                Err(std::io::Error::other(format!(
                    "无法启动 Codex（退出状态：{status}）"
                )))
            }
        }
    }
    #[cfg(windows)]
    {
        let target = windows_codex_launch_target(configured).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "未找到 Codex 桌面应用")
        })?;
        match target {
            WindowsCodexLaunchTarget::Packaged(app_user_model_id) => {
                let arguments = debug_args.join(" ");
                activate_windows_app(&app_user_model_id, &arguments)
            }
            WindowsCodexLaunchTarget::Executable(exe) => {
                Command::new(exe).args(&debug_args).spawn().map(|_| ())
            }
        }
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Err(std::io::Error::other("当前系统暂不支持自动启动 Codex。"))
    }
}

#[cfg(target_os = "macos")]
fn macos_codex_app_path() -> Option<PathBuf> {
    let mut candidates = vec![
        PathBuf::from("/Applications/Codex.app"),
        PathBuf::from("/Applications/ChatGPT.app"),
    ];
    if let Some(home) = dirs::home_dir() {
        candidates.push(home.join("Applications/Codex.app"));
        candidates.push(home.join("Applications/ChatGPT.app"));
    }
    candidates.into_iter().find(|path| path.is_dir())
}

#[cfg(target_os = "macos")]
fn process_named_running(name: &str) -> bool {
    Command::new("/usr/bin/pgrep")
        .args(["-x", name])
        .output()
        .ok()
        .is_some_and(|output| output.status.success())
}

#[cfg(target_os = "macos")]
fn path_running(path: &Path) -> bool {
    // .app 目录按可执行文件路径匹配；可执行文件按自身路径匹配。
    let path = if path.is_dir() {
        path.join("Contents/MacOS").to_string_lossy().to_string()
    } else {
        path.display().to_string()
    };
    let needle = escape_extended_regex(&path);
    Command::new("/usr/bin/pgrep")
        .args(["-f", &needle])
        .output()
        .ok()
        .is_some_and(|output| output.status.success())
}

#[cfg(target_os = "macos")]
fn escape_extended_regex(value: &str) -> String {
    value.chars().fold(
        String::with_capacity(value.len()),
        |mut escaped, character| {
            if matches!(
                character,
                '.' | '[' | ']' | '(' | ')' | '*' | '+' | '?' | '{' | '}' | '|' | '^' | '$' | '\\'
            ) {
                escaped.push('\\');
            }
            escaped.push(character);
            escaped
        },
    )
}

#[cfg(windows)]
fn windows_codex_exe() -> Option<PathBuf> {
    windows_codex_app_package()
        .map(|package| package.executable)
        .or_else(windows_codex_exe_unpacked)
}

#[cfg(windows)]
fn windows_codex_exe_unpacked() -> Option<PathBuf> {
    let local = std::env::var_os("LOCALAPPDATA")?;
    let local = Path::new(&local);
    let standard = [
        local.join("Programs/OpenAI/Codex/Codex.exe"),
        local.join("Programs/Codex/Codex.exe"),
        local.join("Programs/ChatGPT/ChatGPT.exe"),
        local.join("OpenAI/Codex/bin/codex.exe"),
    ]
    .into_iter()
    .find(|path| path.is_file());
    if standard.is_some() {
        return standard;
    }
    // 从注册表卸载信息中查找自定义安装路径
    windows_codex_exe_from_registry()
}

#[cfg(windows)]
struct WindowsCodexPackage {
    executable: PathBuf,
    app_user_model_id: String,
}

#[cfg(windows)]
enum WindowsCodexLaunchTarget {
    Packaged(String),
    Executable(PathBuf),
}

#[cfg(windows)]
fn windows_codex_app_package() -> Option<WindowsCodexPackage> {
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    const PACKAGE_QUERY: &str = concat!(
        "[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); ",
        "$package = Get-AppxPackage -Name 'OpenAI.Codex' | ",
        "Sort-Object Version -Descending | Select-Object -First 1; ",
        "if ($package) { ",
        "$application = (Get-AppxPackageManifest -Package $package).Package.Applications.Application | ",
        "Where-Object { $_.Id -eq 'App' } | Select-Object -First 1; ",
        "if ($application) { ",
        "$package.PackageFamilyName + \"`t\" + $package.InstallLocation + \"`t\" + ",
        "$application.Id + \"`t\" + $application.Executable } }"
    );

    let output = Command::new("powershell.exe")
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            PACKAGE_QUERY,
        ])
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let output = String::from_utf8_lossy(&output.stdout);
    let mut fields = output.trim().splitn(4, '\t');
    let package_family_name = fields.next()?;
    let install_location = fields.next()?;
    let application_id = fields.next()?;
    let executable = fields.next()?;
    if !package_family_name.starts_with("OpenAI.Codex_")
        || install_location.is_empty()
        || application_id.is_empty()
        || executable.is_empty()
    {
        return None;
    }

    Some(WindowsCodexPackage {
        executable: Path::new(install_location).join(executable.replace('/', "\\")),
        app_user_model_id: format!("{package_family_name}!{application_id}"),
    })
}

#[cfg(windows)]
fn windows_codex_launch_target(configured: Option<&str>) -> Option<WindowsCodexLaunchTarget> {
    let configured_path = configured.and_then(|path| validate_codex_app_path(path).ok());
    let package = windows_codex_app_package();

    if let Some(path) = configured_path {
        if let Some(package) =
            package.filter(|package| windows_paths_equal(&path, &package.executable))
        {
            return Some(WindowsCodexLaunchTarget::Packaged(
                package.app_user_model_id,
            ));
        }
        return Some(WindowsCodexLaunchTarget::Executable(path));
    }

    if let Some(package) = package {
        return Some(WindowsCodexLaunchTarget::Packaged(
            package.app_user_model_id,
        ));
    }

    windows_codex_exe_unpacked().map(WindowsCodexLaunchTarget::Executable)
}

#[cfg(windows)]
fn windows_paths_equal(left: &Path, right: &Path) -> bool {
    left.to_string_lossy()
        .trim_end_matches(['\\', '/'])
        .eq_ignore_ascii_case(right.to_string_lossy().trim_end_matches(['\\', '/']))
}

#[cfg(windows)]
fn activate_windows_app(app_user_model_id: &str, arguments: &str) -> std::io::Result<()> {
    let app_user_model_id = app_user_model_id.to_owned();
    let arguments = arguments.to_owned();
    std::thread::Builder::new()
        .name("windows-app-activation".to_owned())
        .spawn(move || activate_windows_app_on_sta(&app_user_model_id, &arguments))
        .map_err(|error| std::io::Error::other(format!("无法创建应用激活线程：{error}")))?
        .join()
        .map_err(|_| std::io::Error::other("Windows 应用激活线程异常退出。"))?
}

#[cfg(windows)]
fn activate_windows_app_on_sta(app_user_model_id: &str, arguments: &str) -> std::io::Result<()> {
    use windows::{
        Win32::{
            System::Com::{
                CLSCTX_LOCAL_SERVER, COINIT_APARTMENTTHREADED, CoCreateInstance, CoInitializeEx,
                CoUninitialize,
            },
            UI::Shell::{AO_NONE, ApplicationActivationManager, IApplicationActivationManager},
        },
        core::PCWSTR,
    };

    struct ComApartment;
    impl Drop for ComApartment {
        fn drop(&mut self) {
            unsafe { CoUninitialize() };
        }
    }

    let initialized = unsafe { CoInitializeEx(None, COINIT_APARTMENTTHREADED) };
    initialized
        .ok()
        .map_err(|error| std::io::Error::other(format!("无法初始化 Windows 应用激活：{error}")))?;
    let _apartment = ComApartment;

    let manager: IApplicationActivationManager =
        unsafe { CoCreateInstance(&ApplicationActivationManager, None, CLSCTX_LOCAL_SERVER) }
            .map_err(|error| {
                std::io::Error::other(format!("无法创建 Windows 应用激活器：{error}"))
            })?;

    let app_user_model_id: Vec<u16> = app_user_model_id.encode_utf16().chain(Some(0)).collect();
    let arguments: Vec<u16> = arguments.encode_utf16().chain(Some(0)).collect();
    unsafe {
        manager.ActivateApplication(
            PCWSTR::from_raw(app_user_model_id.as_ptr()),
            PCWSTR::from_raw(arguments.as_ptr()),
            AO_NONE,
        )
    }
    .map(|_| ())
    .map_err(|error| std::io::Error::other(format!("无法激活 Codex 桌面应用：{error}")))
}

#[cfg(windows)]
fn windows_codex_exe_from_registry() -> Option<PathBuf> {
    use winreg::RegKey;
    use winreg::enums::HKEY_CURRENT_USER;
    let hklm = RegKey::predef(HKEY_CURRENT_USER)
        .open_subkey("Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall")
        .ok()?;
    for entry in hklm.enum_keys() {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let key = match hklm.open_subkey(&entry) {
            Ok(key) => key,
            Err(_) => continue,
        };
        let name: String = key.get_value("DisplayName").unwrap_or_default();
        if !name.eq_ignore_ascii_case("Codex") && !name.eq_ignore_ascii_case("ChatGPT") {
            continue;
        }
        let install_location: String = key.get_value("InstallLocation").unwrap_or_default();
        if install_location.is_empty() {
            continue;
        }
        let dir = Path::new(&install_location);
        for candidate in [dir.join("ChatGPT.exe"), dir.join("Codex.exe")] {
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

#[cfg(windows)]
fn any_process_named_running<'a>(names: impl IntoIterator<Item = &'a str>) -> bool {
    use windows::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
        TH32CS_SNAPPROCESS,
    };

    let names: Vec<String> = names.into_iter().map(|n| n.to_uppercase()).collect();
    if names.is_empty() {
        return false;
    }
    let Ok(snapshot) = (unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) }) else {
        return false;
    };
    let mut entry = PROCESSENTRY32W {
        dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
        ..unsafe { std::mem::zeroed() }
    };
    let mut found = false;
    if unsafe { Process32FirstW(snapshot, &mut entry) }.is_ok() {
        loop {
            let exe = {
                let len = entry.szExeFile.iter().position(|&c| c == 0).unwrap_or(0);
                String::from_utf16_lossy(&entry.szExeFile[..len]).to_uppercase()
            };
            if names.contains(&exe) {
                found = true;
                break;
            }
            if unsafe { Process32NextW(snapshot, &mut entry) }.is_err() {
                break;
            }
        }
    }
    let _ = unsafe { windows::Win32::Foundation::CloseHandle(snapshot) };
    found
}

pub fn os_name() -> &'static str {
    if cfg!(windows) {
        "Windows"
    } else if cfg!(target_os = "macos") {
        "Mac OS"
    } else if cfg!(target_os = "linux") {
        "Linux"
    } else {
        std::env::consts::OS
    }
}

pub fn os_version() -> String {
    #[cfg(windows)]
    return windows_version();

    #[cfg(target_os = "macos")]
    return command_version("/usr/bin/sw_vers", &["-productVersion"]);

    #[cfg(all(unix, not(target_os = "macos")))]
    return command_version("uname", &["-r"]);

    #[allow(unreachable_code)]
    "unknown".into()
}

#[cfg(not(windows))]
fn command_version(program: impl AsRef<Path>, args: &[&str]) -> String {
    Command::new(program.as_ref())
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_owned())
        .filter(|version| !version.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

#[cfg(windows)]
fn windows_version() -> String {
    #[repr(C)]
    struct RtlOsVersionInfo {
        size: u32,
        major: u32,
        minor: u32,
        build: u32,
        platform_id: u32,
        service_pack: [u16; 128],
    }

    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn RtlGetVersion(version_information: *mut RtlOsVersionInfo) -> i32;
    }

    let mut version = RtlOsVersionInfo {
        size: std::mem::size_of::<RtlOsVersionInfo>() as u32,
        major: 0,
        minor: 0,
        build: 0,
        platform_id: 0,
        service_pack: [0; 128],
    };
    // SAFETY: `version` uses the documented RTL_OSVERSIONINFOW layout and
    // remains valid for the duration of the synchronous ntdll call.
    let status = unsafe { RtlGetVersion(&mut version) };
    if status >= 0 && version.major > 0 {
        format!("{}.{}.{}", version.major, version.minor, version.build)
    } else {
        "unknown".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_a_supported_platform_name() {
        assert!(!os_name().is_empty());
        assert!(!os_version().is_empty());
    }

    #[test]
    fn rejects_a_missing_manual_app_path() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("Codex");
        assert!(validate_codex_app_path(&missing.display().to_string()).is_err());
    }

    #[test]
    fn migration_cli_requires_a_configured_desktop_path() {
        let error = codex_cli_command_for_app(None).unwrap_err();
        assert!(error.contains("未配置 Codex Desktop 路径"));
    }

    #[cfg(windows)]
    #[test]
    fn derives_cli_from_custom_windows_desktop_executable() {
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("Custom/Codex.exe");
        let cli = temp.path().join("Custom/resources/codex.exe");
        std::fs::create_dir_all(cli.parent().unwrap()).unwrap();
        std::fs::write(&app, b"desktop").unwrap();
        std::fs::write(&cli, b"cli").unwrap();

        assert_eq!(codex_cli_from_app_path(&app), Some(cli));
    }

    #[cfg(windows)]
    #[test]
    fn accepts_configured_windows_resources_cli_directly() {
        let temp = tempfile::tempdir().unwrap();
        let cli = temp.path().join("Custom/resources/codex.exe");
        std::fs::create_dir_all(cli.parent().unwrap()).unwrap();
        std::fs::write(&cli, b"cli").unwrap();

        assert_eq!(codex_cli_from_app_path(&cli), Some(cli));
    }

    #[cfg(windows)]
    #[test]
    fn does_not_treat_windows_desktop_executable_as_cli_when_bundle_is_missing() {
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("Custom/Codex.exe");
        std::fs::create_dir_all(app.parent().unwrap()).unwrap();
        std::fs::write(&app, b"desktop").unwrap();

        assert_eq!(codex_cli_from_app_path(&app), None);
    }

    #[cfg(unix)]
    #[test]
    fn accepts_a_manual_executable_path() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("Codex");
        std::fs::write(&executable, b"#!/bin/sh\n").unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&executable, permissions).unwrap();

        assert_eq!(
            validate_codex_app_path(&executable.display().to_string()).unwrap(),
            executable
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn accepts_a_macos_app_bundle() {
        let temp = tempfile::tempdir().unwrap();
        let bundle = temp.path().join("ChatGPT.app");
        std::fs::create_dir_all(bundle.join("Contents/MacOS")).unwrap();

        assert_eq!(
            validate_codex_app_path(&bundle.display().to_string()).unwrap(),
            bundle
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn escapes_macos_paths_before_process_matching() {
        assert_eq!(
            escape_extended_regex(r"/Applications/Codex.app/Contents/MacOS/Codex (Beta)+[1]\\test"),
            r"/Applications/Codex\.app/Contents/MacOS/Codex \(Beta\)\+\[1\]\\\\test"
        );
    }
}
