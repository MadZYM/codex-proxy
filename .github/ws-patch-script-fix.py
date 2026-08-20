from pathlib import Path
import re

path = Path('/tmp/apply-websocket.py')
text = path.read_text()

pattern = re.compile(
    r"'''        let responses_url = format!\(\n.*?\n'''",
    re.DOTALL,
)
matches = list(pattern.finditer(text))
if len(matches) != 2:
    raise RuntimeError(f'expected two constructor patch blocks, found {len(matches)}')

correct_target = """'''        let responses_url = format!(
            \"{}{}\",
            cfg.base_url.trim_end_matches('/'),
            cfg.responses_path
        );
        let user_agent = build_user_agent(&cfg.originator, &cfg.cli_version);
'''"""
correct_replacement = """'''        let responses_url = format!(
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
'''"""

for match in reversed(matches):
    replacement = (
        correct_replacement
        if 'responses_websocket_url' in match.group(0)
        else correct_target
    )
    text = text[:match.start()] + replacement + text[match.end():]

path.write_text(text)
