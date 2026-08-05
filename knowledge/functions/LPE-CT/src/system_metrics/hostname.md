---
type: Rust Function
title: hostname
resource: LPE-CT/src/system_metrics.rs#L85-L90
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

`fn hostname() -> String`

# Calls

- [read_trimmed](../../../../functions/LPE-CT/src/system_metrics/read_trimmed.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)