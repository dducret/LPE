---
type: Rust Function
title: cpu_utilization_percent
resource: LPE-CT/src/system_metrics.rs#L99-L118
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/read_trimmed
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/src/system_metrics/percent
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn cpu_utilization_percent() -> Option<f64>`

# Calls

- [read_trimmed](../../../../functions/LPE-CT/src/system_metrics/read_trimmed.md)
- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [percent](../../../../functions/LPE-CT/src/system_metrics/percent.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)