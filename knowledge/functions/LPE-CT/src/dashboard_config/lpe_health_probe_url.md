---
type: Rust Function
title: lpe_health_probe_url
resource: LPE-CT/src/dashboard_config.rs#L502-L514
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dashboard_config/probe_lpe_core_delivery
---

# Signature

`pub(crate) fn lpe_health_probe_url(core_delivery_base_url: &str) -> Result<String, ApiError>`

# Called by

- [probe_lpe_core_delivery](../../../../functions/LPE-CT/src/dashboard_config/probe_lpe_core_delivery.md)