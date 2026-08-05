---
type: Rust Function
title: lpe_bridge_probe_url
resource: LPE-CT/src/dashboard_config.rs#L610-L624
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dashboard_config/probe_lpe_recipient_bridge
---

# Signature

`pub(crate) fn lpe_bridge_probe_url(core_delivery_base_url: &str) -> Result<String, ApiError>`

# Called by

- [probe_lpe_recipient_bridge](../../../../functions/LPE-CT/src/dashboard_config/probe_lpe_recipient_bridge.md)