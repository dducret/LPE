---
type: Python Function
title: imap_command
resource: tools/operations_benchmark.py#L578-L580
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/imap_read_until
  called_by:
  - functions/tools/operations_benchmark/benchmark_imap
---

# Signature

`def imap_command(sock: socket.socket, tag: str, command: str) -> str:`

# Calls

- [imap_read_until](../../../functions/tools/operations_benchmark/imap_read_until.md)

# Called by

- [benchmark_imap](../../../functions/tools/operations_benchmark/benchmark_imap.md)