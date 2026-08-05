---
type: Rust Function
title: percent
resource: LPE-CT/src/system_metrics.rs#L210-L212
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_metrics/cpu_utilization_percent
  - functions/LPE-CT/src/system_metrics/memory_used_percent
  - functions/LPE-CT/src/system_metrics/disk_used_percent
---

# Signature

`fn percent(used: u64, total: u64) -> f64`

# Called by

- [cpu_utilization_percent](../../../../functions/LPE-CT/src/system_metrics/cpu_utilization_percent.md)
- [memory_used_percent](../../../../functions/LPE-CT/src/system_metrics/memory_used_percent.md)
- [disk_used_percent](../../../../functions/LPE-CT/src/system_metrics/disk_used_percent.md)