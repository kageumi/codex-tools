# Codex Tools

<p align="center">
  <img alt="Codex Tools" src="assets/app-icon-master.png" width="96" />
</p>

<p align="center">
  在 Windows 和 macOS 上管理 Codex 的 OpenAI 账号与兼容 Responses API 的第三方服务。
</p>

<p align="center">
  <a href="https://github.com/kageumi/codex-tools/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/kageumi/codex-tools/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://github.com/kageumi/codex-tools/releases/latest"><img alt="GitHub Release" src="https://img.shields.io/github/v/release/kageumi/codex-tools?display_name=tag&sort=semver" /></a>
  <a href="LICENSE"><img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue.svg" /></a>
</p>

Codex Tools 是一个轻量的 Tauri 2 + Rust 桌面应用，直接管理 Codex 配置，不保存聊天正文。

## 界面预览

<p align="center">
  <img alt="Codex Tools 截图" src="assets/screenshot.png" width="90%" />
</p>

## 主要功能

- **账号与 API 服务**：多 OpenAI 账号（设备码授权 / Cookie 导入）与多第三方服务（一个服务一个 Key），随时切换；登录、Token 刷新与连接测试遵循系统代理。
- **模型管理**：保存/切换服务时自动从 `/models` 拉取可用模型，并用 models.dev 补充名称、上下文窗口与简介；可手动或每 10 分钟自动同步。
- **Chat Completions 转换**：DeepSeek、Moonshot、GLM、Qwen 等只有 Chat API 的服务，可在本机 `127.0.0.1:27777` 启动转换代理，把 Responses 请求翻译成 Chat 转发，支持多轮工具调用、图片输入、思考模式及经过 Schema 校验的结构化输出兼容。
- **模型解锁**：通过 CDP 以调试模式启动 Codex 并注入脚本，模型目录只包含当前服务商实际存在的模型，不修改安装文件。
- **用量与费用**：直接读取本机 Codex 会话日志，估算 Token 与美元费用（官方参考价或自定义价格规则），不 Hook、不抓包、不上传。
- **会话诊断**：扫描、修复会话并同步已识别会话的 Provider 元数据。

支持 Windows 10/11 与 macOS 11+（Apple Silicon）。

## 下载与安装

从 [Releases](https://github.com/kageumi/codex-tools/releases/latest) 下载对应平台的安装包：

| 平台                | 发布产物                | 系统要求                |
| ------------------- | ----------------------- | ----------------------- |
| Windows x64         | `_x64-setup.exe` 安装包 | Windows 10/11、WebView2 |
| macOS Apple Silicon | `_aarch64.dmg` 磁盘映像 | macOS 11 或更高版本     |

> [!IMPORTANT]
> 当前自动构建的安装包尚未配置代码签名，系统可能显示“未知发布者”或阻止打开。请核对 Release 来源与 `SHA256SUMS.txt`；不确定时请从源码构建。

macOS 首次启动若被 Gatekeeper 阻止，在“系统设置 → 隐私与安全性”中确认来源即可。若核对校验和后仍误报损坏，只移除该应用的隔离标记：

```shell
xattr -dr com.apple.quarantine "/Applications/Codex Tools.app"
```

启动后应用会自动寻找 Codex 配置目录和 CLI，也可用 `CODEX_HOME` 与 `CODEX_BIN` 指定位置。

## 快速开始

1. 启动 Codex Tools，在设置页确认 Codex 路径；未识别时可手动选择（macOS 选 `.app`，Windows 选可执行文件）。
2. 添加 OpenAI 账号（设备码 / Cookie）或兼容 Responses / Chat Completions API 的服务。
3. 测试连接、同步模型并选择当前使用的账号或服务。
4. 从概览点击“启动 Codex”；需要第三方模型目录时可在设置页使用模型解锁。

切换账号、服务或修复会话归属前，应用会要求先退出正在运行的 Codex 实例。也请避免用其他工具同时编辑 `config.toml` 或 `auth.json`；检测到外部修改时，本次切换会停止并要求重试。

## 配置行为

第三方服务激活后，应用把所选服务写入 Codex 配置：

```toml
model_provider = "custom"

[model_providers.custom]
name = "<Provider 名称>"
wire_api = "responses"
requires_openai_auth = true
base_url = "https://api.example.com/v1"

[model_providers.custom.http_headers]
X-Custom-Header = "<可选请求头>"
```

对应的 `auth.json` 会重建为只含当前第三方凭据。切换回官方账号时，应用删除受管的 `custom` 字段并写入所选账号凭据；MCP、Skills、Hooks、沙箱及其他未知配置保持不变。

## 数据与安全

用量与费用页面只读取本机 Codex 会话日志，美元金额是按官方参考价或自定义价格规则计算的**估算值**，不代表实际账单。凭据按产品约定明文保存在本机，请勿上传或分享：

| 平台    | 数据位置                                                                 |
| ------- | ------------------------------------------------------------------------ |
| Windows | 安装目录或可执行文件同级的 `data/app.json`                               |
| macOS   | `~/Library/Application Support/io.github.irasutoya.codex-tools/app.json` |

激活第三方账号后，API Key 还会写入 Codex 的 `auth.json`。可用 `CODEX_TOOLS_DATA_DIR` 覆盖数据目录。

## 本地开发

需要 Bun 1.4、Rust 1.85+ 及当前平台的 [Tauri 2 构建依赖](https://v2.tauri.app/start/prerequisites/)（Windows 另需 WebView2、NSIS；macOS 需 Xcode CLT）。

```shell
git clone https://github.com/kageumi/codex-tools.git
cd codex-tools
bun install --frozen-lockfile
bun run dev
```

提交前运行完整检查：

```shell
bun run check
cargo fmt --manifest-path src-tauri/Cargo.toml --all -- --check
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets --locked -- -D warnings
cargo test --manifest-path src-tauri/Cargo.toml --locked
```

本地打包（Windows x64 输出 NSIS 安装包，macOS 输出 DMG）：

```shell
bun run dist:win
bun run dist:mac:arm64
```

## 项目状态

项目仍处于早期预览阶段。请在 [Issues](https://github.com/kageumi/codex-tools/issues) 中报告问题；提交前请移除 API Key、OAuth token、`auth.json`、`credentials.json` 与会话正文。

## 许可证

[MIT](LICENSE)
