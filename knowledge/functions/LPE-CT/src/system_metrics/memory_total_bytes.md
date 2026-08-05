---
type: Rust Function
title: memory_total_bytes
resource: LPE-CT/src/system_metrics.rs#L144-L146
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/meminfo_kib
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn memory_total_bytes() -> Option<u64>`

# Calls

- [meminfo_kib](../../../../functions/LPE-CT/src/system_metrics/meminfo_kib.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)