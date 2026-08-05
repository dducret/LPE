---
type: Rust Function
title: cpuinfo_value
resource: LPE-CT/src/system_metrics.rs#L157-L159
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/key_value_file
  called_by:
  - functions/LPE-CT/src/system_metrics/processor_type
  - functions/LPE-CT/src/system_metrics/processor_speed_mhz
---

# Signature

`fn cpuinfo_value(name: &str) -> Option<String>`

# Calls

- [key_value_file](../../../../functions/LPE-CT/src/system_metrics/key_value_file.md)

# Called by

- [processor_type](../../../../functions/LPE-CT/src/system_metrics/processor_type.md)
- [processor_speed_mhz](../../../../functions/LPE-CT/src/system_metrics/processor_speed_mhz.md)