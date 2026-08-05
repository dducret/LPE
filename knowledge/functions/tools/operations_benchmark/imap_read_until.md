---
type: Python Function
title: imap_read_until
resource: tools/operations_benchmark.py#L583-L598
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/operations_benchmark/imap_read_until_greeting
  - functions/tools/operations_benchmark/imap_command
---

# Signature

`def imap_read_until(sock: socket.socket, tag: str | None) -> str:`

# Called by

- [imap_read_until_greeting](../../../functions/tools/operations_benchmark/imap_read_until_greeting.md)
- [imap_command](../../../functions/tools/operations_benchmark/imap_command.md)