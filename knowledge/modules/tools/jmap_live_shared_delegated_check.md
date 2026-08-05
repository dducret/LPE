---
type: Python Module
title: jmap_live_shared_delegated_check
resource: tools/jmap_live_shared_delegated_check.py#L1-L450
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/base64
  - external/json
  - external/os
  - external/socket
  - external/ssl
  - external/struct
  - external/sys
  - external/time
  - external/urllib-error
  - external/urllib-parse
  - external/urllib-request
  - external/dataclasses
  - external/typing
---

# Contains

- [AccountLogin](../../classes/tools/jmap_live_shared_delegated_check/AccountLogin.md)
- [env](../../functions/tools/jmap_live_shared_delegated_check/env.md)
- [bool_env](../../functions/tools/jmap_live_shared_delegated_check/bool_env.md)
- [http_json](../../functions/tools/jmap_live_shared_delegated_check/http_json.md)
- [require_status](../../functions/tools/jmap_live_shared_delegated_check/require_status.md)
- [login](../../functions/tools/jmap_live_shared_delegated_check/login.md)
- [jmap](../../functions/tools/jmap_live_shared_delegated_check/jmap.md)
- [recv_exact](../../functions/tools/jmap_live_shared_delegated_check/recv_exact.md)
- [websocket_url](../../functions/tools/jmap_live_shared_delegated_check/websocket_url.md)
- [ws_connect](../../functions/tools/jmap_live_shared_delegated_check/ws_connect.md)
- [ws_send_text](../../functions/tools/jmap_live_shared_delegated_check/ws_send_text.md)
- [ws_send_pong](../../functions/tools/jmap_live_shared_delegated_check/ws_send_pong.md)
- [ws_recv_frame](../../functions/tools/jmap_live_shared_delegated_check/ws_recv_frame.md)
- [ws_recv_text_json](../../functions/tools/jmap_live_shared_delegated_check/ws_recv_text_json.md)
- [enable_push_snapshot](../../functions/tools/jmap_live_shared_delegated_check/enable_push_snapshot.md)
- [replay_push](../../functions/tools/jmap_live_shared_delegated_check/replay_push.md)
- [upsert_grants](../../functions/tools/jmap_live_shared_delegated_check/upsert_grants.md)
- [cleanup_grants](../../functions/tools/jmap_live_shared_delegated_check/cleanup_grants.md)
- [assert_grantee_jmap_visibility](../../functions/tools/jmap_live_shared_delegated_check/assert_grantee_jmap_visibility.md)
- [assert_push_replay](../../functions/tools/jmap_live_shared_delegated_check/assert_push_replay.md)
- [main](../../functions/tools/jmap_live_shared_delegated_check/main.md)

# Imports

- `base64`
- `json`
- `os`
- `socket`
- `ssl`
- `struct`
- `sys`
- `time`
- `urllib.error`
- `urllib.parse`
- `urllib.request`
- `dataclasses`
- `typing`