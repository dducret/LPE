---
type: Python Function
title: encode_wbxml_node
resource: tools/operations_benchmark.py#L385-L403
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/operations_benchmark/encode_wbxml
---

# Signature

`def encode_wbxml_node(node: dict[str, Any], current_page: int, out: bytearray) -> int:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [encode_wbxml](../../../functions/tools/operations_benchmark/encode_wbxml.md)