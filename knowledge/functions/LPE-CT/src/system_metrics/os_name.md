---
type: Rust Function
title: os_name
resource: LPE-CT/src/system_metrics.rs#L140-L142
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/os_release_value
  - functions/LPE-CT/src/system_metrics/read_trimmed
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn os_name() -> Option<String>`

# Calls

- [os_release_value](../../../../functions/LPE-CT/src/system_metrics/os_release_value.md)
- [read_trimmed](../../../../functions/LPE-CT/src/system_metrics/read_trimmed.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)