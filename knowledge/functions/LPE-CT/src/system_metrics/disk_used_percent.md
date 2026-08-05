---
type: Rust Function
title: disk_used_percent
resource: LPE-CT/src/system_metrics.rs#L502-L513
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/percent
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn disk_used_percent(path: &Path) -> Option<f64>`

# Calls

- [percent](../../../../functions/LPE-CT/src/system_metrics/percent.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)