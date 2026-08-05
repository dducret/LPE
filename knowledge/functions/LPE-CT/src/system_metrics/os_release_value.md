---
type: Rust Function
title: os_release_value
resource: LPE-CT/src/system_metrics.rs#L170-L172
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/key_value_file
  called_by:
  - functions/LPE-CT/src/system_metrics/os_name
---

# Signature

`fn os_release_value(name: &str) -> Option<String>`

# Calls

- [key_value_file](../../../../functions/LPE-CT/src/system_metrics/key_value_file.md)

# Called by

- [os_name](../../../../functions/LPE-CT/src/system_metrics/os_name.md)