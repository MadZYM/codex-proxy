from pathlib import Path
import re

path = Path('/tmp/apply-websocket.py')
text = path.read_text()


def replace_two_blocks(pattern_text: str, target: str, replacement: str, discriminator: str) -> None:
    global text
    pattern = re.compile(pattern_text, re.DOTALL)
    matches = list(pattern.finditer(text))
    if len(matches) != 2:
        raise RuntimeError(f'expected two patch blocks for {discriminator}, found {len(matches)}')
    for match in reversed(matches):
        fixed = replacement if discriminator in match.group(0) else target
        text = text[:match.start()] + fixed + text[match.end():]


replace_two_blocks(
    r"'''        let responses_url = format!\(\n.*?\n'''",
    """'''        let responses_url = format!(
            \"{}{}\",
            cfg.base_url.trim_end_matches('/'),
            cfg.responses_path
        );
        let user_agent = build_user_agent(&cfg.originator, &cfg.cli_version);
'''""",
    """'''        let responses_url = format!(
            \"{}{}\",
            cfg.base_url.trim_end_matches('/'),
            cfg.responses_path
        );
        let responses_websocket_url = websocket_url(&responses_url);
        let websocket_http = build_websocket_http_client(cfg)
            .map_err(|error| format!(\"building upstream websocket client failed: {error}\"));
        if let Err(error) = &websocket_http {
            tracing::error!(%error, \"upstream websocket client unavailable\");
        }
        let user_agent = build_user_agent(&cfg.originator, &cfg.cli_version);
'''""",
    'responses_websocket_url',
)

replace_two_blocks(
    r"'''        Self \{\n.*?\n'''",
    """'''        Self {
            http,
            pool,
            next: AtomicUsize::new(0),
            account_cooldown: Duration::from_secs(cfg.account_cooldown_secs),
            responses_url,
            originator: cfg.originator.clone(),
'''""",
    """'''        Self {
            http,
            websocket_http,
            pool,
            next: AtomicUsize::new(0),
            account_cooldown: Duration::from_secs(cfg.account_cooldown_secs),
            responses_url,
            responses_websocket_url,
            originator: cfg.originator.clone(),
'''""",
    'websocket_http',
)

path.write_text(text)
