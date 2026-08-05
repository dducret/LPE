---
type: Python Function
title: replay_push
resource: tools/jmap_live_shared_delegated_check.py#L273-L289
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/jmap_live_shared_delegated_check/ws_recv_text_json
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/jmap_live_shared_delegated_check/main
---

# Signature

`def replay_push(account: AccountLogin, push_state: str) -> dict[str, Any]:`

# Calls

- [ws_recv_text_json](../../../functions/tools/jmap_live_shared_delegated_check/ws_recv_text_json.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [main](../../../functions/tools/jmap_live_shared_delegated_check/main.md)