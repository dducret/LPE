---
type: Python Function
title: smtp_send_data
resource: tools/operations_benchmark.py#L772-L774
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/smtp_read_reply
  called_by:
  - functions/tools/operations_benchmark/benchmark_smtp_data
---

# Signature

`def smtp_send_data(sock: socket.socket, message: str) -> str:`

# Calls

- [smtp_read_reply](../../../functions/tools/operations_benchmark/smtp_read_reply.md)

# Called by

- [benchmark_smtp_data](../../../functions/tools/operations_benchmark/benchmark_smtp_data.md)