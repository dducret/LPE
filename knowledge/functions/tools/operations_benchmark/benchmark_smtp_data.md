---
type: Python Function
title: benchmark_smtp_data
resource: tools/operations_benchmark.py#L708-L739
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/smtp_connect
  - functions/tools/operations_benchmark/smtp_read_reply
  - functions/tools/operations_benchmark/timed
  - functions/tools/operations_benchmark/smtp_send_data
  called_by:
  - functions/tools/operations_benchmark/main
---

# Signature

`def benchmark_smtp_data(iterations: int) -> list[Measurement]:`

# Calls

- [smtp_connect](../../../functions/tools/operations_benchmark/smtp_connect.md)
- [smtp_read_reply](../../../functions/tools/operations_benchmark/smtp_read_reply.md)
- [timed](../../../functions/tools/operations_benchmark/timed.md)
- [smtp_send_data](../../../functions/tools/operations_benchmark/smtp_send_data.md)

# Called by

- [main](../../../functions/tools/operations_benchmark/main.md)