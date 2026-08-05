---
type: Python Function
title: http_bytes
resource: tools/operations_benchmark.py#L200-L212
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/operations_benchmark/benchmark_activesync
---

# Signature

`def http_bytes( url: str, method: str, body: bytes | None, headers: dict[str, str], timeout: int = 30, ) -> tuple[int, bytes, dict[str, str]]:`

# Called by

- [benchmark_activesync](../../../functions/tools/operations_benchmark/benchmark_activesync.md)