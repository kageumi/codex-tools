//! Shared local Codex Desktop CDP discovery and expression evaluation.

use crate::{models::AppError, storage::Store};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_tungstenite::tungstenite::Message;

/// 探测已有 CDP 端口的候选列表：上次随机端口（运行时拼入）以及
/// 常见调试端口（codex++ 9229、常规 DevTools 端口等），便于复用用户
/// 已开启的调试实例。不再包含固定端口：本应用每次都以随机端口启动，
/// 避免端口可被本机其他进程预测。
pub(crate) const PROBE_PORTS: &[u16] = &[9229, 9222, 9334, 9335];
const PROBE_TIMEOUT: Duration = Duration::from_millis(400);
const MAX_CDP_PROBE_BYTES: usize = 256 * 1024;
const EVALUATE_TIMEOUT: Duration = Duration::from_secs(5);

/// 构造调试端口候选列表：上次启动用的随机端口优先，再补上常见调试端口。
/// 端口不再固定，本机其他进程无法预测本应用的调试端口。
pub(crate) fn debug_probe_ports(store: &Store) -> Vec<u16> {
    debug_probe_ports_for(store.last_debug_port().ok().flatten())
}

/// Reuse an already read preference when taking an atomic connection snapshot.
pub(crate) fn debug_probe_ports_for(last_debug_port: Option<u16>) -> Vec<u16> {
    let mut ports = Vec::with_capacity(PROBE_PORTS.len() + 1);
    if let Some(last) = last_debug_port {
        ports.push(last);
    }
    for &port in PROBE_PORTS {
        if !ports.contains(&port) {
            ports.push(port);
        }
    }
    ports
}

/// 分配一个随机的本机端口：让系统从临时端口范围挑选（借用后立即释放），
/// 避免固定端口被本机其他进程探测/复用。
pub(crate) fn pick_debug_port() -> u16 {
    std::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .ok()
        .and_then(|listener| listener.local_addr().ok())
        .map(|address| address.port())
        .unwrap_or(PROBE_PORTS[0])
}
/// Accept only local uncredentialed WebSocket URLs on an explicit debug port.
/// Discovery also checks that the returned endpoint belongs to its probe port.
pub(crate) fn validate_debug_ws_url(ws_url: &str, expected_port: Option<u16>) -> bool {
    let Ok(url) = reqwest::Url::parse(ws_url) else {
        return false;
    };
    url.scheme() == "ws"
        && matches!(url.host_str(), Some("127.0.0.1" | "localhost"))
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url
            .port()
            .is_some_and(|port| port != 0 && expected_port.is_none_or(|expected| port == expected))
}

/// Interpret Runtime.evaluate replies consistently for one-shot and persistent callers.
pub(crate) fn parse_evaluation_response(value: &Value) -> Result<Value, AppError> {
    if let Some(error) = value.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("未知错误");
        return Err(AppError::Internal(format!("Codex 调试指令失败：{message}")));
    }
    if let Some(details) = value.pointer("/result/exceptionDetails") {
        let message = details
            .pointer("/exception/description")
            .and_then(Value::as_str)
            .or_else(|| details.get("text").and_then(Value::as_str))
            .unwrap_or("未知错误");
        return Err(AppError::Internal(format!(
            "Codex 渲染进程执行脚本失败：{message}"
        )));
    }
    let result = value
        .pointer("/result/result")
        .ok_or_else(|| AppError::Internal("Codex 调试响应缺少求值结果。".into()))?;
    if let Some(value) = result.get("value") {
        return Ok(value.clone());
    }
    if result.get("type").and_then(Value::as_str) == Some("undefined")
        || result.get("subtype").and_then(Value::as_str) == Some("null")
    {
        return Ok(Value::Null);
    }
    Err(AppError::Internal("Codex 调试响应无法转换为 JSON。".into()))
}

/// 在所有候选端口上查找 Codex 的 CDP 端点，并复用探测结果里的页面
/// WebSocket 地址，避免调用方再次探测同一端口。
pub(crate) async fn find_codex_debug_page_on(ports: &[u16]) -> Option<(u16, String)> {
    for &port in ports {
        if let Some(ws_url) = probe_port(port).await {
            return Some((port, ws_url));
        }
    }
    None
}

#[derive(Debug, Deserialize)]
struct CdpTarget {
    #[serde(rename = "type")]
    target_type: String,
    #[serde(default)]
    url: String,
    #[serde(default, rename = "webSocketDebuggerUrl")]
    web_socket_debugger_url: Option<String>,
}

fn is_codex_page_target(target: &CdpTarget, port: u16) -> bool {
    target.target_type == "page"
        && target.url.trim_start().starts_with("app://-")
        && target
            .web_socket_debugger_url
            .as_deref()
            .is_some_and(|url| validate_debug_ws_url(url, Some(port)))
}

/// 从目标列表中挑选要注入的页面：优先主窗口（`app://-/index.html`，无查询
/// 参数），避免注入到头像浮层等弹窗页面；没有主窗口时才回退到第一个页面。
fn pick_codex_page_ws(targets: Vec<CdpTarget>, port: u16) -> Option<String> {
    let pages = targets
        .into_iter()
        .filter(|target| is_codex_page_target(target, port))
        .collect::<Vec<_>>();
    if pages.is_empty() {
        return None;
    }
    pages
        .iter()
        .find(|target| !target.url.contains('?'))
        .or_else(|| pages.first())
        .and_then(|target| target.web_socket_debugger_url.clone())
}

/// 对单个端口发起 `GET /json` 探测，返回 Codex 页面目标的 WebSocket 地址。
pub(crate) async fn probe_port(port: u16) -> Option<String> {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let mut stream = tokio::time::timeout_at(deadline.into(), TcpStream::connect(address))
        .await
        .ok()?
        .ok()?;
    let request =
        format!("GET /json HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    tokio::time::timeout_at(deadline.into(), stream.write_all(request.as_bytes()))
        .await
        .ok()?
        .ok()?;
    let mut body = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.read(&mut chunk)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(read)) => {
                body.extend_from_slice(&chunk[..read]);
                if body.len() > MAX_CDP_PROBE_BYTES {
                    break;
                }
            }
            _ => break,
        }
    }
    let response = String::from_utf8_lossy(&body);
    let header_end = response.find("\r\n\r\n")?;
    if !response[..header_end].contains(" 200 ") {
        return None;
    }
    let targets: Vec<CdpTarget> = serde_json::from_str(response[header_end + 4..].trim()).ok()?;
    pick_codex_page_ws(targets, port)
}

/// 通过 CDP WebSocket 在 Codex 渲染进程执行表达式，返回求值结果
/// （`returnByValue` + `awaitPromise`）。
pub(crate) async fn evaluate(ws_url: &str, expression: &str) -> Result<Value, AppError> {
    if !validate_debug_ws_url(ws_url, None) {
        return Err(AppError::Internal("Codex 调试端点必须是本机地址。".into()));
    }
    let (mut socket, _response) =
        tokio::time::timeout(EVALUATE_TIMEOUT, tokio_tungstenite::connect_async(ws_url))
            .await
            .map_err(|_| AppError::Internal("连接 Codex 调试端口超时。".into()))?
            .map_err(|error| AppError::Internal(format!("无法连接 Codex 调试端口：{error}")))?;
    let request = json!({
        "id": 1,
        "method": "Runtime.evaluate",
        "params": {
            "expression": expression,
            "returnByValue": true,
            "awaitPromise": true,
        }
    });
    tokio::time::timeout(
        EVALUATE_TIMEOUT,
        socket.send(Message::Text(request.to_string())),
    )
    .await
    .map_err(|_| AppError::Internal("向 Codex 发送调试指令超时。".into()))?
    .map_err(|error| AppError::Internal(format!("无法向 Codex 发送调试指令：{error}")))?;
    tokio::time::timeout(EVALUATE_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .ok_or_else(|| AppError::Internal("Codex 调试端口连接中断。".into()))?
                .map_err(|error| AppError::Internal(format!("Codex 调试端口连接错误：{error}")))?;
            let Message::Text(text) = message else {
                continue;
            };
            let value: Value = serde_json::from_str(&text)
                .map_err(|error| AppError::Internal(format!("Codex 调试响应格式无效：{error}")))?;
            if value.get("id").and_then(Value::as_u64) != Some(1) {
                continue;
            }
            return parse_evaluation_response(&value);
        }
    })
    .await
    .map_err(|_| AppError::Internal("等待 Codex 响应超时。".into()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn probe_candidates_prioritize_saved_port_without_duplicates() {
        assert_eq!(debug_probe_ports_for(None), PROBE_PORTS);
        assert_eq!(debug_probe_ports_for(Some(9222)), [9222, 9229, 9334, 9335]);
        assert_eq!(
            debug_probe_ports_for(Some(54321)),
            [54321, 9229, 9222, 9334, 9335]
        );
    }

    #[tokio::test]
    async fn probe_detects_codex_page_target() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            let body = r#"[{"id":"page-1","type":"page","url":"app://-/index.html","webSocketDebuggerUrl":"ws://127.0.0.1:PORT/devtools/page/abc"},{"id":"other","type":"other","url":"about:blank"}]"#;
            let body = body.replace("PORT", &port.to_string());
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let ws_url = probe_port(port).await;
        let expected = format!("ws://127.0.0.1:{port}/devtools/page/abc");
        assert_eq!(ws_url.as_deref(), Some(expected.as_str()));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn probe_prefers_main_window_over_popup_targets() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            // 弹窗目标排在最前，主窗口排在后面：必须选中主窗口。
            let body = format!(
                r#"[{{"id":"popup","type":"page","url":"app://-/index.html?initialRoute=%2Favatar-overlay","webSocketDebuggerUrl":"ws://127.0.0.1:{port}/devtools/page/popup"}},{{"id":"main","type":"page","url":"app://-/index.html","webSocketDebuggerUrl":"ws://127.0.0.1:{port}/devtools/page/main"}}]"#
            );
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let expected = format!("ws://127.0.0.1:{port}/devtools/page/main");
        assert_eq!(probe_port(port).await.as_deref(), Some(expected.as_str()));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn probe_ignores_non_codex_targets_and_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            let body = r#"[{"id":"web","type":"page","url":"https://example.test/","webSocketDebuggerUrl":"ws://127.0.0.1/devtools/page/abc"}]"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        assert!(probe_port(port).await.is_none());
        server.join().unwrap();
        assert!(probe_port(1).await.is_none());
    }

    #[test]
    fn rejects_remote_or_mismatched_debug_endpoints() {
        for url in [
            "ws://example.test:9222/devtools/page/a",
            "ws://127.0.0.1:9333/devtools/page/a",
            "ws://user@localhost:9222/devtools/page/a",
            "wss://localhost:9222/devtools/page/a",
            "ws://localhost/devtools/page/a",
            "ws://127.0.0.1:9222/devtools/page/a#fragment",
        ] {
            assert!(!validate_debug_ws_url(url, Some(9222)), "{url}");
        }
        assert!(validate_debug_ws_url(
            "ws://127.0.0.1:9222/devtools/page/a",
            Some(9222)
        ));
        assert!(validate_debug_ws_url(
            "ws://localhost:9222/devtools/page/a",
            Some(9222)
        ));
    }

    #[test]
    fn target_selection_skips_forged_websocket_addresses() {
        let targets = serde_json::from_value(json!([
            {"type":"page","url":"app://-/index.html","webSocketDebuggerUrl":"ws://example.test:9222/devtools/page/main"},
            {"type":"page","url":"app://-/index.html","webSocketDebuggerUrl":"ws://localhost:9333/devtools/page/main"},
            {"type":"page","url":"app://-/index.html?initialRoute=popup","webSocketDebuggerUrl":"ws://127.0.0.1:9222/devtools/page/popup"}
        ]))
        .unwrap();
        assert_eq!(
            pick_codex_page_ws(targets, 9222).as_deref(),
            Some("ws://127.0.0.1:9222/devtools/page/popup")
        );
    }

    #[test]
    fn parses_evaluation_values_and_failures() {
        assert_eq!(
            parse_evaluation_response(
                &json!({"result":{"result":{"type":"object","value":{"ok":true}}}})
            )
            .unwrap(),
            json!({"ok":true})
        );
        assert_eq!(
            parse_evaluation_response(&json!({"result":{"result":{"type":"undefined"}}})).unwrap(),
            Value::Null
        );
        assert!(
            parse_evaluation_response(&json!({"error":{"code":-32601,"message":"unsupported"}}))
                .unwrap_err()
                .to_string()
                .contains("unsupported")
        );
        assert!(parse_evaluation_response(&json!({"result":{"exceptionDetails":{"text":"Uncaught","exception":{"description":"bridge missing"}}}})).unwrap_err().to_string().contains("bridge missing"));
        assert!(parse_evaluation_response(&json!({"result":{}})).is_err());
        assert!(
            parse_evaluation_response(
                &json!({"result":{"result":{"type":"number","unserializableValue":"NaN"}}})
            )
            .is_err()
        );
    }
}
