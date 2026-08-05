---
type: Python Function
title: assert_push_replay
resource: tools/jmap_live_shared_delegated_check.py#L401-L413
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/jmap_live_shared_delegated_check/main
---

# Signature

`def assert_push_replay(state_change: dict[str, Any]) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [main](../../../functions/tools/jmap_live_shared_delegated_check/main.md)