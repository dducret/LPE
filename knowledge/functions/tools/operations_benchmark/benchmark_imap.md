---
type: Python Function
title: benchmark_imap
resource: tools/operations_benchmark.py#L529-L564
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/imap_connect
  - functions/tools/operations_benchmark/imap_read_until_greeting
  - functions/tools/operations_benchmark/imap_command
  - functions/tools/operations_benchmark/timed
  - functions/tools/operations_benchmark/imap_exists_count
  called_by:
  - functions/tools/operations_benchmark/main
---

# Signature

`def benchmark_imap(email: str, password: str, iterations: int) -> list[Measurement]:`

# Calls

- [imap_connect](../../../functions/tools/operations_benchmark/imap_connect.md)
- [imap_read_until_greeting](../../../functions/tools/operations_benchmark/imap_read_until_greeting.md)
- [imap_command](../../../functions/tools/operations_benchmark/imap_command.md)
- [timed](../../../functions/tools/operations_benchmark/timed.md)
- [imap_exists_count](../../../functions/tools/operations_benchmark/imap_exists_count.md)

# Called by

- [main](../../../functions/tools/operations_benchmark/main.md)