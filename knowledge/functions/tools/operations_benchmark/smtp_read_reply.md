---
type: Python Function
title: smtp_read_reply
resource: tools/operations_benchmark.py#L749-L764
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/operations_benchmark/benchmark_smtp_data
  - functions/tools/operations_benchmark/smtp_command
  - functions/tools/operations_benchmark/smtp_send_data
---

# Signature

`def smtp_read_reply(sock: socket.socket) -> str:`

# Called by

- [benchmark_smtp_data](../../../functions/tools/operations_benchmark/benchmark_smtp_data.md)
- [smtp_command](../../../functions/tools/operations_benchmark/smtp_command.md)
- [smtp_send_data](../../../functions/tools/operations_benchmark/smtp_send_data.md)