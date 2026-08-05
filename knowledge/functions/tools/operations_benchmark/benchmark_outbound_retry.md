---
type: Python Function
title: benchmark_outbound_retry
resource: tools/operations_benchmark.py#L777-L797
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/require_env
  called_by:
  - functions/tools/operations_benchmark/main
---

# Signature

`def benchmark_outbound_retry(iterations: int) -> list[Measurement]:`

# Calls

- [require_env](../../../functions/tools/operations_benchmark/require_env.md)

# Called by

- [main](../../../functions/tools/operations_benchmark/main.md)