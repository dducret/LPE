---
type: Python Function
title: run_section
resource: tools/operations_benchmark.py#L800-L805
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/ews_live_smoke_check/EwsClient/call
  called_by:
  - functions/tools/operations_benchmark/main
---

# Signature

`def run_section(name: str, call: Callable[[], list[Measurement] | Measurement]) -> list[Measurement]:`

# Calls

- [call](../../../functions/tools/ews_live_smoke_check/EwsClient/call.md)

# Called by

- [main](../../../functions/tools/operations_benchmark/main.md)