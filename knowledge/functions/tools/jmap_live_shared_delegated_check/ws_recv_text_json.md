---
type: Python Function
title: ws_recv_text_json
resource: tools/jmap_live_shared_delegated_check.py#L242-L252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/jmap_live_shared_delegated_check/ws_recv_frame
  called_by:
  - functions/tools/jmap_live_shared_delegated_check/enable_push_snapshot
  - functions/tools/jmap_live_shared_delegated_check/replay_push
---

# Signature

`def ws_recv_text_json(sock: ssl.SSLSocket) -> dict[str, Any]:`

# Calls

- [ws_recv_frame](../../../functions/tools/jmap_live_shared_delegated_check/ws_recv_frame.md)

# Called by

- [enable_push_snapshot](../../../functions/tools/jmap_live_shared_delegated_check/enable_push_snapshot.md)
- [replay_push](../../../functions/tools/jmap_live_shared_delegated_check/replay_push.md)