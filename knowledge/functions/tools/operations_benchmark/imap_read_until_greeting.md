---
type: Python Function
title: imap_read_until_greeting
resource: tools/operations_benchmark.py#L574-L575
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/imap_read_until
  called_by:
  - functions/tools/operations_benchmark/benchmark_imap
---

# Signature

`def imap_read_until_greeting(sock: socket.socket) -> str:`

# Calls

- [imap_read_until](../../../functions/tools/operations_benchmark/imap_read_until.md)

# Called by

- [benchmark_imap](../../../functions/tools/operations_benchmark/benchmark_imap.md)