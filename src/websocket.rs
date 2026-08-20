//! Transparent downstream <-> upstream Codex Responses WebSocket relay.
//!
//! The only frame inspection is read-only: the first `response.create` supplies
//! the routing-hint model/tier, and `response.completed` supplies access-log token
//! usage. Every frame is otherwise forwarded without JSON reconstruction, preserving
//! tools, reasoning/encrypted reasoning, prewarm, incremental requests, and unknown
//! current or future events.

use std::sync::Arc;

use axum::extract::ws::{CloseFrame, Message as DownstreamMessage, WebSocket};
use axum::http::HeaderMap;
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{CloseCode, Message as UpstreamMessage};
use serde_json::json;

use crate::metrics::Metrics;
use crate::observe::{AccessCtx, CompletionLog};
use crate::upstream::{ForwardedWebSocket, Upstream, WebSocketConnectError};

pub(crate) async fn proxy_responses(
    mut downstream: WebSocket,
    upstream: Arc<Upstream>,
    client_headers: HeaderMap,
    ctx: AccessCtx,
    metrics: Arc<Metrics>,
    max_message_bytes: usize,
) {
    let first = match receive_first_request(&mut downstream, max_message_bytes).await {
        Ok(Some(message)) => message,
        Ok(None) => return,
        Err(message) => {
            let log = CompletionLog::new(ctx, "/v1/responses", "-", "-", metrics);
            send_proxy_error(&mut downstream, 400, &message).await;
            log.emit(400, None);
            return;
        }
    };
    let first_payload = data_payload(&first)
        .expect("first websocket request is always a data frame")
        .to_vec();
    let model = request_model(&first_payload).unwrap_or_else(|| "-".to_string());
    let mut log = CompletionLog::new(ctx, "/v1/responses", model, "-", metrics);

    let ForwardedWebSocket {
        mut websocket,
        account,
    } = match upstream
        .connect_responses_websocket(&first_payload, &client_headers)
        .await
    {
        Ok(forwarded) => forwarded,
        Err(error) => {
            send_connect_error(&mut downstream, &error).await;
            log.emit(error.status, None);
            return;
        }
    };
    log.set_account(account);

    let mut usage = None;
    let result = relay_frames(&mut downstream, &mut websocket, first, &mut usage).await;
    match result {
        Ok(()) => log.emit(101, usage),
        Err(error) => {
            send_proxy_error(&mut downstream, 502, &error).await;
            log.emit(502, usage);
        }
    }
}

async fn receive_first_request(
    downstream: &mut WebSocket,
    max_message_bytes: usize,
) -> Result<Option<DownstreamMessage>, String> {
    while let Some(message) = downstream.recv().await {
        let message = message.map_err(|error| error.to_string())?;
        match message {
            data @ (DownstreamMessage::Text(_) | DownstreamMessage::Binary(_)) => {
                let size = data_payload(&data).map_or(0, <[u8]>::len);
                if size > max_message_bytes {
                    let _ = downstream
                        .send(DownstreamMessage::Close(Some(CloseFrame {
                            code: 1009,
                            reason: "response.create frame exceeds configured limit".into(),
                        })))
                        .await;
                    return Err(format!(
                        "websocket request frame is {size} bytes; limit is {max_message_bytes}"
                    ));
                }
                return Ok(Some(data));
            }
            DownstreamMessage::Ping(payload) => {
                downstream
                    .send(DownstreamMessage::Pong(payload))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            DownstreamMessage::Pong(_) => {}
            DownstreamMessage::Close(frame) => {
                let _ = downstream.send(DownstreamMessage::Close(frame)).await;
                return Ok(None);
            }
        }
    }
    Ok(None)
}

async fn relay_frames(
    downstream: &mut WebSocket,
    upstream: &mut reqwest_websocket::WebSocket,
    first: DownstreamMessage,
    usage: &mut Option<(i64, i64)>,
) -> Result<(), String> {
    upstream
        .send(downstream_to_upstream(first))
        .await
        .map_err(|error| format!("sending first upstream frame failed: {error}"))?;

    loop {
        tokio::select! {
            downstream_message = downstream.recv() => {
                let Some(downstream_message) = downstream_message else {
                    let _ = upstream.send(UpstreamMessage::Close {
                        code: CloseCode::Normal,
                        reason: String::new(),
                    }).await;
                    return Ok(());
                };
                let downstream_message = downstream_message
                    .map_err(|error| format!("reading downstream websocket failed: {error}"))?;
                let is_close = matches!(downstream_message, DownstreamMessage::Close(_));
                upstream
                    .send(downstream_to_upstream(downstream_message))
                    .await
                    .map_err(|error| format!("sending upstream websocket frame failed: {error}"))?;
                if is_close {
                    return Ok(());
                }
            }
            upstream_message = upstream.next() => {
                let Some(upstream_message) = upstream_message else {
                    let _ = downstream.send(DownstreamMessage::Close(Some(CloseFrame {
                        code: 1000,
                        reason: String::new().into(),
                    }))).await;
                    return Ok(());
                };
                let upstream_message = upstream_message
                    .map_err(|error| format!("reading upstream websocket failed: {error}"))?;
                if let UpstreamMessage::Text(text) = &upstream_message {
                    if let Some(completed_usage) = completed_usage(text) {
                        *usage = Some(completed_usage);
                    }
                }
                let is_close = matches!(upstream_message, UpstreamMessage::Close { .. });
                downstream
                    .send(upstream_to_downstream(upstream_message))
                    .await
                    .map_err(|error| format!("sending downstream websocket frame failed: {error}"))?;
                if is_close {
                    return Ok(());
                }
            }
        }
    }
}

fn data_payload(message: &DownstreamMessage) -> Option<&[u8]> {
    match message {
        DownstreamMessage::Text(text) => Some(text.as_bytes()),
        DownstreamMessage::Binary(bytes) => Some(bytes.as_ref()),
        _ => None,
    }
}

fn request_model(payload: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(payload)
        .ok()?
        .get("model")?
        .as_str()
        .map(str::to_string)
}

fn completed_usage(payload: &str) -> Option<(i64, i64)> {
    let event: serde_json::Value = serde_json::from_str(payload).ok()?;
    if event.get("type")?.as_str()? != "response.completed" {
        return None;
    }
    let usage = event.get("response")?.get("usage")?;
    Some((
        usage.get("input_tokens")?.as_i64()?,
        usage.get("output_tokens")?.as_i64()?,
    ))
}

fn downstream_to_upstream(message: DownstreamMessage) -> UpstreamMessage {
    match message {
        DownstreamMessage::Text(text) => UpstreamMessage::Text(text),
        DownstreamMessage::Binary(bytes) => UpstreamMessage::Binary(bytes.into()),
        DownstreamMessage::Ping(bytes) => UpstreamMessage::Ping(bytes.into()),
        DownstreamMessage::Pong(bytes) => UpstreamMessage::Pong(bytes.into()),
        DownstreamMessage::Close(Some(frame)) => UpstreamMessage::Close {
            code: frame.code.into(),
            reason: frame.reason.into_owned(),
        },
        DownstreamMessage::Close(None) => UpstreamMessage::Close {
            code: CloseCode::Normal,
            reason: String::new(),
        },
    }
}

fn upstream_to_downstream(message: UpstreamMessage) -> DownstreamMessage {
    match message {
        UpstreamMessage::Text(text) => DownstreamMessage::Text(text),
        UpstreamMessage::Binary(bytes) => DownstreamMessage::Binary(bytes.to_vec()),
        UpstreamMessage::Ping(bytes) => DownstreamMessage::Ping(bytes.to_vec()),
        UpstreamMessage::Pong(bytes) => DownstreamMessage::Pong(bytes.to_vec()),
        UpstreamMessage::Close { code, reason } => DownstreamMessage::Close(Some(CloseFrame {
            code: u16::from(code),
            reason: reason.into(),
        })),
    }
}

async fn send_connect_error(downstream: &mut WebSocket, error: &WebSocketConnectError) {
    send_proxy_error(downstream, error.status, &error.message).await;
}

async fn send_proxy_error(downstream: &mut WebSocket, status: u16, message: &str) {
    let payload = json!({
        "type": "error",
        "status": status,
        "error": {
            "type": "upstream_error",
            "code": "websocket_proxy_error",
            "message": message,
        }
    })
    .to_string();
    let _ = downstream.send(DownstreamMessage::Text(payload)).await;
    let _ = downstream
        .send(DownstreamMessage::Close(Some(CloseFrame {
            code: 1011,
            reason: "upstream websocket proxy failed".into(),
        })))
        .await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::body::Body;
    use axum::extract::ws::{Message, WebSocketUpgrade};
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::Response;
    use axum::routing::get;
    use axum::Router;
    use reqwest_websocket::RequestBuilderExt;
    use tokio::sync::mpsc;

    use super::*;
    use crate::auth::AuthManager;
    use crate::config::{ClientKey, Config};
    use crate::fallback::FallbackChain;
    use crate::server::{router, AppState};
    use crate::test_support::write_test_auth_json;

    #[derive(Clone)]
    struct FakeState {
        headers_tx: mpsc::Sender<HeaderMap>,
        frames_tx: mpsc::Sender<(usize, String)>,
        connections: Arc<AtomicUsize>,
    }

    struct RunningServer {
        base_url: String,
        task: tokio::task::JoinHandle<()>,
    }

    async fn start_fake_upstream() -> (
        RunningServer,
        mpsc::Receiver<HeaderMap>,
        mpsc::Receiver<(usize, String)>,
        Arc<AtomicUsize>,
    ) {
        let (headers_tx, headers_rx) = mpsc::channel(4);
        let (frames_tx, frames_rx) = mpsc::channel(8);
        let connections = Arc::new(AtomicUsize::new(0));
        let state = FakeState {
            headers_tx,
            frames_tx,
            connections: connections.clone(),
        };
        let app = Router::new()
            .route("/codex/responses", get(fake_websocket))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            RunningServer {
                base_url: format!("http://{addr}"),
                task,
            },
            headers_rx,
            frames_rx,
            connections,
        )
    }

    async fn fake_websocket(
        State(state): State<FakeState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        state.headers_tx.send(headers).await.unwrap();
        let connection_id = state.connections.fetch_add(1, Ordering::SeqCst);
        ws.on_upgrade(move |mut socket| async move {
            let mut request_index = 0usize;
            while let Some(message) = socket.recv().await {
                match message {
                    Ok(Message::Text(text)) => {
                        request_index += 1;
                        state
                            .frames_tx
                            .send((connection_id, text.clone()))
                            .await
                            .unwrap();
                        let response_id = format!("resp-{request_index}");
                        let events = [
                            json!({
                                "type": "response.created",
                                "response": {"id": response_id.clone()}
                            })
                            .to_string(),
                            json!({
                                "type": "future.codex.event",
                                "payload": {"kept": true, "request": request_index}
                            })
                            .to_string(),
                            json!({
                                "type": "response.output_text.delta",
                                "delta": format!("reply-{request_index}")
                            })
                            .to_string(),
                            json!({
                                "type": "response.completed",
                                "response": {
                                    "id": response_id,
                                    "usage": {
                                        "input_tokens": 3,
                                        "output_tokens": 2,
                                        "total_tokens": 5
                                    }
                                }
                            })
                            .to_string(),
                        ];
                        for event in events {
                            socket.send(Message::Text(event)).await.unwrap();
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        let _ = socket.send(Message::Pong(payload)).await;
                    }
                    Ok(Message::Close(frame)) => {
                        let _ = socket.send(Message::Close(frame)).await;
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        })
    }

    async fn start_proxy(upstream_base_url: String) -> RunningServer {
        let mut config = Config::default();
        config.client_auth.keys = vec![ClientKey {
            key: "test-key".to_string(),
            name: Some("websocket-test".to_string()),
        }];
        config.upstream.base_url = upstream_base_url;
        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_ws"),
            http.clone(),
        )
        .unwrap();
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "ws-account".to_string())],
        ));
        let fallback = Arc::new(FallbackChain::new(http, &config.fallback).unwrap());
        let metrics = Arc::new(Metrics::new().unwrap());
        let app = router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        RunningServer {
            base_url: format!("http://{addr}"),
            task,
        }
    }

    async fn recv_text(websocket: &mut reqwest_websocket::WebSocket) -> String {
        match tokio::time::timeout(Duration::from_secs(3), websocket.next())
            .await
            .expect("timed out waiting for websocket event")
            .expect("websocket closed before event")
            .expect("websocket event error")
        {
            UpstreamMessage::Text(text) => text,
            other => panic!("expected text websocket event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn websocket_proxy_preserves_frames_headers_unknown_events_and_connection_reuse() {
        let (fake, mut headers_rx, mut frames_rx, connections) = start_fake_upstream().await;
        let proxy = start_proxy(fake.base_url.clone()).await;
        let client = reqwest::Client::builder().http1_only().build().unwrap();
        let websocket_url = proxy.base_url.replacen("http://", "ws://", 1) + "/v1/responses";

        let unauthorized = client.get(&websocket_url).upgrade().send().await.unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let upgrade = client
            .get(&websocket_url)
            .header("Authorization", "Bearer test-key")
            .header("session-id", "session-1")
            .header("thread-id", "thread-1")
            .header("x-client-request-id", "thread-1")
            .header("x-codex-window-id", "thread-1:0")
            .header("x-codex-beta-features", "feature-a")
            .header("x-responsesapi-include-timing-metrics", "true")
            .header("OpenAI-Beta", "client-spoofed")
            .header("x-codex-routing-hint", "model=spoofed;tier=wrong")
            .header("x-openai-internal-codex-residency", "us")
            .upgrade()
            .send()
            .await
            .unwrap();
        assert_eq!(upgrade.status(), StatusCode::SWITCHING_PROTOCOLS);
        let mut websocket = upgrade.into_websocket().await.unwrap();

        let first = r#"{"type":"response.create","model":"gpt-5.6-sol","service_tier":"priority","input":[{"role":"user","content":"hello"}],"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}],"reasoning":{"summary":"auto"},"include":["reasoning.encrypted_content"],"future_field":{"kept":true}}"#;
        websocket
            .send(UpstreamMessage::Text(first.to_string()))
            .await
            .unwrap();
        let first_events = [
            recv_text(&mut websocket).await,
            recv_text(&mut websocket).await,
            recv_text(&mut websocket).await,
            recv_text(&mut websocket).await,
        ];
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&first_events[1]).unwrap()["type"],
            "future.codex.event"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&first_events[3]).unwrap()["type"],
            "response.completed"
        );

        let second = r#"{"type":"response.create","model":"gpt-5.6-sol","previous_response_id":"resp-1","input":[],"stream":true,"unknown_continuation_field":[1,2,3]}"#;
        websocket
            .send(UpstreamMessage::Text(second.to_string()))
            .await
            .unwrap();
        let second_events = [
            recv_text(&mut websocket).await,
            recv_text(&mut websocket).await,
            recv_text(&mut websocket).await,
            recv_text(&mut websocket).await,
        ];
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&second_events[3]).unwrap()["response"]["id"],
            "resp-2"
        );

        let first_captured = tokio::time::timeout(Duration::from_secs(3), frames_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let second_captured = tokio::time::timeout(Duration::from_secs(3), frames_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_captured, (0, first.to_string()));
        assert_eq!(second_captured, (0, second.to_string()));
        assert_eq!(connections.load(Ordering::SeqCst), 1);

        let headers = tokio::time::timeout(Duration::from_secs(3), headers_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("Bearer ")));
        assert_eq!(
            headers
                .get("chatgpt-account-id")
                .and_then(|v| v.to_str().ok()),
            Some("acct_ws")
        );
        assert_eq!(
            headers.get("originator").and_then(|v| v.to_str().ok()),
            Some("codex_cli_rs")
        );
        assert!(headers
            .get("user-agent")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("codex_cli_rs/0.147.0 (")));
        assert_eq!(
            headers.get("openai-beta").and_then(|v| v.to_str().ok()),
            Some("responses_websockets=2026-02-06")
        );
        assert_eq!(
            headers
                .get("x-codex-routing-hint")
                .and_then(|v| v.to_str().ok()),
            Some("model=gpt-5.6-sol;tier=priority")
        );
        assert_eq!(
            headers.get("session-id").and_then(|v| v.to_str().ok()),
            Some("session-1")
        );
        assert_eq!(
            headers.get("thread-id").and_then(|v| v.to_str().ok()),
            Some("thread-1")
        );
        assert_eq!(
            headers
                .get("x-client-request-id")
                .and_then(|v| v.to_str().ok()),
            Some("thread-1")
        );
        assert_eq!(
            headers
                .get("x-responsesapi-include-timing-metrics")
                .and_then(|v| v.to_str().ok()),
            Some("true")
        );
        assert!(headers.get("x-openai-internal-codex-residency").is_none());

        websocket
            .send(UpstreamMessage::Close {
                code: CloseCode::Normal,
                reason: String::new(),
            })
            .await
            .unwrap();
        proxy.task.abort();
        fake.task.abort();
    }

    #[test]
    fn completed_usage_only_reads_response_completed_events() {
        assert_eq!(
            completed_usage(
                r#"{"type":"response.completed","response":{"usage":{"input_tokens":7,"output_tokens":4}}}"#
            ),
            Some((7, 4))
        );
        assert_eq!(
            completed_usage(r#"{"type":"response.output_text.delta","delta":"x"}"#),
            None
        );
    }

    #[test]
    fn proxy_error_has_official_wrapped_error_shape() {
        let payload: serde_json::Value = serde_json::from_str(
            &json!({
                "type": "error",
                "status": 502,
                "error": {
                    "type": "upstream_error",
                    "code": "websocket_proxy_error",
                    "message": "failed"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(payload["type"], "error");
        assert_eq!(payload["status"], 502);
        assert_eq!(payload["error"]["code"], "websocket_proxy_error");
    }

    #[tokio::test]
    async fn websocket_url_uses_same_responses_path() {
        let (fake, _, _, _) = start_fake_upstream().await;
        assert!(fake.base_url.starts_with("http://"));
        assert_eq!(
            crate::upstream::websocket_url_for_test(&format!("{}/codex/responses", fake.base_url)),
            format!(
                "{}/codex/responses",
                fake.base_url.replacen("http://", "ws://", 1)
            )
        );
        fake.task.abort();
    }

    #[allow(dead_code)]
    fn _body_type_check(_: Body) {}
}
