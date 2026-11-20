//! Opt-in protocol acceptance test using the installed Codex, never the user's CODEX_HOME.
use super::{build_model_catalog_with_windows_for_provider, write_model_catalog};
use crate::models::ProviderProfile;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

struct AppServer {
    child: Child,
    stdin: ChildStdin,
    messages: mpsc::Receiver<Value>,
    notifications: VecDeque<Value>,
    next_id: u64,
}

impl AppServer {
    fn start(binary: &std::ffi::OsStr, home: &Path, cwd: &Path) -> Self {
        let mut command = Command::new(binary);
        command
            .args(["app-server", "--stdio"])
            .env("CODEX_HOME", home)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(
                std::fs::File::create(home.join("app-server.stderr.log")).unwrap(),
            ));
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x0800_0000);
        }
        let mut child = command.spawn().expect("start isolated Codex app-server");
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, messages) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Ok(message) = serde_json::from_str(&line) {
                    if sender.send(message).is_err() {
                        break;
                    }
                }
            }
        });
        Self {
            child,
            stdin,
            messages,
            notifications: VecDeque::new(),
            next_id: 0,
        }
    }

    fn send(&mut self, message: Value) {
        writeln!(self.stdin, "{message}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn receive(&self) -> Value {
        self.messages
            .recv_timeout(Duration::from_secs(30))
            .expect("Codex must respond within 30 seconds")
    }

    fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"id": id, "method": method, "params": params}));
        loop {
            let message = self.receive();
            if message["id"].as_u64() == Some(id) && message.get("method").is_none() {
                assert!(message.get("error").is_none(), "{method}: {message}");
                return message["result"].clone();
            }
            assert!(
                message.get("id").is_none(),
                "unexpected server request: {message}"
            );
            self.notifications.push_back(message);
        }
    }

    fn completed(&mut self, turn_id: &str) {
        loop {
            let message = self
                .notifications
                .pop_front()
                .unwrap_or_else(|| self.receive());
            if message["method"] == "turn/completed" && message["params"]["turn"]["id"] == turn_id {
                assert_eq!(
                    message["params"]["turn"]["status"], "completed",
                    "{message}"
                );
                return;
            }
            assert!(
                message.get("id").is_none(),
                "unexpected server request: {message}"
            );
        }
    }
}

impl Drop for AppServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CODEX_BIN pointing to an installed Codex; uses only an isolated local mock"]
async fn fast_mode_native_cli_sends_selected_tier_and_preserves_reasoning() {
    let binary =
        std::env::var_os("CODEX_BIN").expect("set CODEX_BIN to the installed Codex executable");
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = requests.clone();
    let router = axum::Router::new().route(
        "/v1/responses",
        axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let captured = captured.clone();
            async move {
                let mut requests = captured.lock().unwrap();
                requests.push(body);
                let id = format!("resp_{}", requests.len());
                let output = json!({
                    "id": format!("msg_{}", requests.len()),
                    "type": "message", "role": "assistant", "status": "completed",
                    "content": [{"type": "output_text", "text": "OK", "annotations": []}]
                });
                let events = [
                    json!({"type": "response.created", "response": {
                        "id": id, "object": "response", "status": "in_progress", "output": []
                    }}),
                    json!({"type": "response.output_item.done", "output_index": 0, "item": output}),
                    json!({"type": "response.completed", "response": {
                        "id": id, "object": "response", "status": "completed", "output": [output],
                        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
                    }}),
                ];
                let stream = events
                    .iter()
                    .map(|event| {
                        format!(
                            "event: {}\ndata: {event}\n\n",
                            event["type"].as_str().unwrap()
                        )
                    })
                    .collect::<String>();
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    stream,
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("codex");
    let cwd = temp.path().join("workspace");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&cwd).unwrap();
    let provider: ProviderProfile = serde_json::from_value(json!({
        "id": "local-mock", "name": "Local mock", "baseUrl": format!("http://{address}/v1"),
        "availableModels": ["relay-fast-test", "relay-priority-test"],
        "modelsDevMeta": {
            "relay-fast-test": {
                "serviceTiers": [{"id": "fast", "name": "Fast", "description": ""}],
                "supportedReasoningLevels": ["low", "high"]
            },
            "relay-priority-test": {
                "serviceTiers": [{"id": "priority", "name": "Fast", "description": ""}],
                "supportedReasoningLevels": ["low", "high"]
            }
        }
    }))
    .unwrap();
    let catalog = build_model_catalog_with_windows_for_provider(&provider, &home);
    write_model_catalog(&home, &catalog).unwrap();
    let config = format!(
        "model = \"relay-fast-test\"\nmodel_provider = \"custom\"\n\
         model_catalog_json = \"model-catalogs/codex-tools.json\"\n\
         service_tier = \"default\"\nmodel_reasoning_effort = \"high\"\n\
         approval_policy = \"never\"\nsandbox_mode = \"read-only\"\n\
         [features]\nfast_mode = true\n\
         [analytics]\nenabled = false\n\
         [model_providers.custom]\nname = \"Local mock\"\nwire_api = \"responses\"\n\
         requires_openai_auth = true\nbase_url = \"http://{address}/v1\"\n"
    );
    std::fs::write(home.join("config.toml"), &config).unwrap();
    std::fs::write(
        home.join("auth.json"),
        r#"{"OPENAI_API_KEY":"local-mock-key"}"#,
    )
    .unwrap();
    let mut client = AppServer::start(&binary, &home, &cwd);
    client.rpc(
        "initialize",
        json!({
            "clientInfo": {"name": "codex_tools_test", "version": "1"},
            "capabilities": {"experimentalApi": true}
        }),
    );
    client.send(json!({"method": "initialized"}));
    let models = client.rpc("model/list", json!({"includeHidden": true}));
    let cases = [
        ("relay-fast-test", "fast"),
        ("relay-priority-test", "priority"),
    ];
    for (model_id, fast_tier) in cases {
        let model = models["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|model| model["model"] == model_id)
            .expect("generated provider model must be visible to native Codex");
        assert_eq!(model["serviceTiers"][0]["id"], fast_tier, "{model}");
        assert_eq!(model["additionalSpeedTiers"], json!(["fast"]));
        assert_eq!(model["defaultServiceTier"], Value::Null);
        let thread = client.rpc(
            "thread/start",
            json!({
                "model": model_id, "cwd": cwd, "approvalPolicy": "never",
                "sandbox": "read-only", "ephemeral": true
            }),
        );
        for tier in [fast_tier, "default"] {
            // Per-turn selection must not use the legacy, persistent serviceTier override.
            let turn = client.rpc(
                "turn/start",
                json!({
                    "threadId": thread["thread"]["id"], "serviceTierForTurn": tier, "effort": "high",
                    "input": [{"type": "text", "text": "Reply OK without using tools."}]
                }),
            );
            client.completed(turn["turn"]["id"].as_str().unwrap());
        }
    }
    drop(client);
    server.abort();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 4, "one upstream request per turn");
    for (index, (model_id, fast_tier)) in cases.iter().enumerate() {
        let pair = &requests[index * 2..index * 2 + 2];
        assert_eq!(pair[0]["service_tier"], *fast_tier);
        assert!(
            pair[1]
                .get("service_tier")
                .is_none_or(|tier| tier.is_null() || tier == "default"),
            "standard mode must not send a fast tier: {}",
            pair[1]
        );
        for request in pair {
            assert_eq!(request["model"], *model_id);
            assert_eq!(request["reasoning"]["effort"], "high");
        }
    }
    assert_eq!(
        std::fs::read_to_string(home.join("config.toml")).unwrap(),
        config
    );
}
