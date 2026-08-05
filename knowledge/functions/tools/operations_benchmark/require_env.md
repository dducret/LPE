---
type: Python Function
title: require_env
resource: tools/operations_benchmark.py#L140-L144
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/operations_benchmark/benchmark_outbound_retry
---

# Signature

`def require_env(name: str, fallback: str | None = None) -> str:`

# Called by

- [benchmark_outbound_retry](../../../functions/tools/operations_benchmark/benchmark_outbound_retry.md)