---
type: Rust Function
title: item_for_name
resource: LPE-CT/src/host_logs.rs#L221-L238
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/is_previewable
  called_by:
  - functions/LPE-CT/src/host_logs/list
---

# Signature

`fn item_for_name(name: &str, path: &Path) -> HostLogItem`

# Calls

- [is_previewable](../../../../functions/LPE-CT/src/host_logs/is_previewable.md)

# Called by

- [list](../../../../functions/LPE-CT/src/host_logs/list.md)