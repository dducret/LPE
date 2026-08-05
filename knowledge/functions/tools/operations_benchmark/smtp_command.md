---
type: Python Function
title: smtp_command
resource: tools/operations_benchmark.py#L767-L769
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/smtp_read_reply
---

# Signature

`def smtp_command(sock: socket.socket, command: str) -> str:`

# Calls

- [smtp_read_reply](../../../functions/tools/operations_benchmark/smtp_read_reply.md)