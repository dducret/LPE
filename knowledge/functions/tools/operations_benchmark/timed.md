---
type: Python Function
title: timed
resource: tools/operations_benchmark.py#L161-L164
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/ews_live_smoke_check/EwsClient/call
  called_by:
  - functions/tools/operations_benchmark/benchmark_jmap
  - functions/tools/operations_benchmark/benchmark_imap
  - functions/tools/operations_benchmark/benchmark_activesync
  - functions/tools/operations_benchmark/benchmark_smtp_data
  - functions/tools/operations_benchmark/main
---

# Signature

`def timed(call: Callable[[], Any]) -> tuple[float, Any]:`

# Calls

- [call](../../../functions/tools/ews_live_smoke_check/EwsClient/call.md)

# Called by

- [benchmark_jmap](../../../functions/tools/operations_benchmark/benchmark_jmap.md)
- [benchmark_imap](../../../functions/tools/operations_benchmark/benchmark_imap.md)
- [benchmark_activesync](../../../functions/tools/operations_benchmark/benchmark_activesync.md)
- [benchmark_smtp_data](../../../functions/tools/operations_benchmark/benchmark_smtp_data.md)
- [main](../../../functions/tools/operations_benchmark/main.md)