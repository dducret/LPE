---
type: Python Function
title: ws_recv_frame
resource: tools/jmap_live_shared_delegated_check.py#L226-L239
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/jmap_live_shared_delegated_check/ws_recv_text_json
---

# Signature

`def ws_recv_frame(sock: ssl.SSLSocket) -> tuple[int, bytes]:`

# Called by

- [ws_recv_text_json](../../../functions/tools/jmap_live_shared_delegated_check/ws_recv_text_json.md)