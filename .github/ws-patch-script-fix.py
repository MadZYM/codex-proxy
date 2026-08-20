from pathlib import Path

path = Path('/tmp/apply-websocket.py')
text = path.read_text()

old_target = """'''        let responses_url = format!(
        \"{}{}\",
        cfg.base_url.trim_end_matches('/'),
        cfg.responses_path
    );
    let user_agent = build_user_agent(&cfg.originator, &cfg.cli_version);
'''
"""
new_target = """'''        let responses_url = format!(
            \"{}{}\",
            cfg.base_url.trim_end_matches('/'),
            cfg.responses_path
        );
        let user_agent = build_user_agent(&cfg.originator, &cfg.cli_version);
'''
"""
old_replacement = """'''        let responses_url = format!(
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
'''
"""
new_replacement = """'''        let responses_url = format!(
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
'''
"""

for old, new in ((old_target, new_target), (old_replacement, new_replacement)):
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f'expected one malformed constructor block, found {count}')
    text = text.replace(old, new)

path.write_text(text)
