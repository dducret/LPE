---
type: Rust Function
title: processor_speed_mhz
resource: LPE-CT/src/system_metrics.rs#L136-L138
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

`fn processor_speed_mhz() -> Option<f64>`

# Calls

- [cpuinfo_value](../../../../functions/LPE-CT/src/system_metrics/cpuinfo_value.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)