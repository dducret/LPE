---
type: Rust Function
title: load_averages
resource: LPE-CT/src/system_metrics.rs#L120-L128
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/read_trimmed
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn load_averages() -> Option<[f64; 3]>`

# Calls

- [read_trimmed](../../../../functions/LPE-CT/src/system_metrics/read_trimmed.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)