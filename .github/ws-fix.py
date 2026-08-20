from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, found {count}: {old!r}")
    file.write_text(text.replace(old, new))


def replace_last(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    index = text.rfind(old)
    if index < 0:
        raise RuntimeError(f"{path}: expected at least one match: {old!r}")
    file.write_text(text[:index] + new + text[index + len(old):])


replace_once(
    "src/upstream.rs",
    """        if !matches!(
            attempt,
            WebSocketAttempt::Rejected {
                status: reqwest::StatusCode::UNAUTHORIZED,
                ..
            }
        ) {
""",
    """        if !matches!(
            &attempt,
            WebSocketAttempt::Rejected {
                status: reqwest::StatusCode::UNAUTHORIZED,
                ..
            }
        ) {
""",
)

replace_once(
    "src/upstream.rs",
    "Connected(reqwest_websocket::WebSocket),",
    "Connected(Box<reqwest_websocket::WebSocket>),",
)
replace_once(
    "src/upstream.rs",
    "return Ok(ForwardedWebSocket { websocket, account });",
    "return Ok(ForwardedWebSocket {\n                            websocket: *websocket,\n                            account,\n                        });",
)
replace_last(
    "src/upstream.rs",
    "Ok(WebSocketAttempt::Connected(websocket))",
    "Ok(WebSocketAttempt::Connected(Box::new(websocket)))",
)

replace_once(
    "src/websocket.rs",
    '"response": {"id": response_id}\n',
    '"response": {"id": response_id.clone()}\n',
)

replace_once(
    "src/websocket.rs",
    """                if let UpstreamMessage::Text(text) = &upstream_message
                    && let Some(completed_usage) = completed_usage(text)
                {
                    *usage = Some(completed_usage);
                }
""",
    """                if let UpstreamMessage::Text(text) = &upstream_message {
                    if let Some(completed_usage) = completed_usage(text) {
                        *usage = Some(completed_usage);
                    }
                }
""",
)
