---
type: Python Function
title: markdown_report
resource: tools/operations_benchmark.py#L808-L827
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/Measurement/summary
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/operations_benchmark/main
---

# Signature

`def markdown_report(results: list[Measurement]) -> str:`

# Calls

- [summary](../../../functions/tools/operations_benchmark/Measurement/summary.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [main](../../../functions/tools/operations_benchmark/main.md)