---
type: Python Function
title: benchmark_activesync
resource: tools/operations_benchmark.py#L612-L694
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/jmap_inbox_mailbox_id
  - functions/tools/operations_benchmark/basic_header
  - functions/tools/operations_benchmark/wbxml_node
  - functions/tools/operations_benchmark/timed
  - functions/tools/operations_benchmark/http_bytes
  called_by:
  - functions/tools/operations_benchmark/main
---

# Signature

`def benchmark_activesync(base_url: str, account: AccountLogin, password: str, iterations: int) -> list[Measurement]:`

# Calls

- [jmap_inbox_mailbox_id](../../../functions/tools/operations_benchmark/jmap_inbox_mailbox_id.md)
- [basic_header](../../../functions/tools/operations_benchmark/basic_header.md)
- [wbxml_node](../../../functions/tools/operations_benchmark/wbxml_node.md)
- [timed](../../../functions/tools/operations_benchmark/timed.md)
- [http_bytes](../../../functions/tools/operations_benchmark/http_bytes.md)

# Called by

- [main](../../../functions/tools/operations_benchmark/main.md)