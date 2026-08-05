---
type: Python Function
title: benchmark_jmap
resource: tools/operations_benchmark.py#L449-L504
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/timed
  - functions/tools/operations_benchmark/method_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/tools/operations_benchmark/websocket_push_enable_round_trip
  called_by:
  - functions/tools/operations_benchmark/main
---

# Signature

`def benchmark_jmap(account: AccountLogin, iterations: int) -> list[Measurement]:`

# Calls

- [timed](../../../functions/tools/operations_benchmark/timed.md)
- [method_response](../../../functions/tools/operations_benchmark/method_response.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [websocket_push_enable_round_trip](../../../functions/tools/operations_benchmark/websocket_push_enable_round_trip.md)

# Called by

- [main](../../../functions/tools/operations_benchmark/main.md)