---
type: Rust Function
title: key_value_file
resource: LPE-CT/src/system_metrics.rs#L174-L184
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_metrics/cpuinfo_value
  - functions/LPE-CT/src/system_metrics/meminfo_kib
  - functions/LPE-CT/src/system_metrics/os_release_value
---

# Signature

`fn key_value_file(path: &str, name: &str) -> Option<String>`

# Called by

- [cpuinfo_value](../../../../functions/LPE-CT/src/system_metrics/cpuinfo_value.md)
- [meminfo_kib](../../../../functions/LPE-CT/src/system_metrics/meminfo_kib.md)
- [os_release_value](../../../../functions/LPE-CT/src/system_metrics/os_release_value.md)