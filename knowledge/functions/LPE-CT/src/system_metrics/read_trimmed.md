---
type: Rust Function
title: read_trimmed
resource: LPE-CT/src/system_metrics.rs#L186-L191
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_metrics/hostname
  - functions/LPE-CT/src/system_metrics/uptime_seconds
  - functions/LPE-CT/src/system_metrics/cpu_utilization_percent
  - functions/LPE-CT/src/system_metrics/load_averages
  - functions/LPE-CT/src/system_metrics/os_name
---

# Signature

`fn read_trimmed(path: &str) -> Option<String>`

# Called by

- [hostname](../../../../functions/LPE-CT/src/system_metrics/hostname.md)
- [uptime_seconds](../../../../functions/LPE-CT/src/system_metrics/uptime_seconds.md)
- [cpu_utilization_percent](../../../../functions/LPE-CT/src/system_metrics/cpu_utilization_percent.md)
- [load_averages](../../../../functions/LPE-CT/src/system_metrics/load_averages.md)
- [os_name](../../../../functions/LPE-CT/src/system_metrics/os_name.md)