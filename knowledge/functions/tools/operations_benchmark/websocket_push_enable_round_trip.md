---
type: Python Function
title: websocket_push_enable_round_trip
resource: tools/operations_benchmark.py#L507-L526
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/ws_recv_text
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/operations_benchmark/ws_send_close
  called_by:
  - functions/tools/operations_benchmark/benchmark_jmap
---

# Signature

`def websocket_push_enable_round_trip(account: AccountLogin) -> dict[str, Any]:`

# Calls

- [ws_recv_text](../../../functions/tools/operations_benchmark/ws_recv_text.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [ws_send_close](../../../functions/tools/operations_benchmark/ws_send_close.md)

# Called by

- [benchmark_jmap](../../../functions/tools/operations_benchmark/benchmark_jmap.md)