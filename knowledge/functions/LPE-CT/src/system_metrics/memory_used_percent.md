---
type: Rust Function
title: memory_used_percent
resource: LPE-CT/src/system_metrics.rs#L148-L155
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/meminfo_kib
  - functions/LPE-CT/src/system_metrics/percent
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn memory_used_percent() -> Option<f64>`

# Calls

- [meminfo_kib](../../../../functions/LPE-CT/src/system_metrics/meminfo_kib.md)
- [percent](../../../../functions/LPE-CT/src/system_metrics/percent.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)