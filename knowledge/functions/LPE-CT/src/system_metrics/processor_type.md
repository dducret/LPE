---
type: Rust Function
title: processor_type
resource: LPE-CT/src/system_metrics.rs#L130-L134
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/cpuinfo_value
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn processor_type() -> Option<String>`

# Calls

- [cpuinfo_value](../../../../functions/LPE-CT/src/system_metrics/cpuinfo_value.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)