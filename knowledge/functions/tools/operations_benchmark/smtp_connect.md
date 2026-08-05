---
type: Python Function
title: smtp_connect
resource: tools/operations_benchmark.py#L742-L746
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/operations_benchmark/benchmark_smtp_data
---

# Signature

`def smtp_connect(host: str, port: int, use_tls: bool) -> socket.socket:`

# Called by

- [benchmark_smtp_data](../../../functions/tools/operations_benchmark/benchmark_smtp_data.md)