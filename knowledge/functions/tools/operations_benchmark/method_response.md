---
type: Python Function
title: method_response
resource: tools/operations_benchmark.py#L254-L258
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/operations_benchmark/benchmark_jmap
  - functions/tools/operations_benchmark/jmap_inbox_mailbox_id
---

# Signature

`def method_response(body: dict[str, Any], name: str) -> dict[str, Any]:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [benchmark_jmap](../../../functions/tools/operations_benchmark/benchmark_jmap.md)
- [jmap_inbox_mailbox_id](../../../functions/tools/operations_benchmark/jmap_inbox_mailbox_id.md)