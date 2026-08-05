---
type: Rust Function
title: disk_total_bytes
resource: LPE-CT/src/system_metrics.rs#L498-L500
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn disk_total_bytes(path: &Path) -> Option<u64>`

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)