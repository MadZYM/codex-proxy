from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, found {count}: {old!r}")
    file.write_text(text.replace(old, new))


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
