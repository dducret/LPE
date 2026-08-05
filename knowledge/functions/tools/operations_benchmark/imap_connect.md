---
type: Python Function
title: imap_connect
resource: tools/operations_benchmark.py#L567-L571
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/operations_benchmark/benchmark_imap
---

# Signature

`def imap_connect(host: str, port: int, use_tls: bool) -> socket.socket:`

# Called by

- [benchmark_imap](../../../functions/tools/operations_benchmark/benchmark_imap.md)