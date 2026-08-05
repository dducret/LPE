---
type: Rust Function
title: normalize_verification_type
resource: LPE-CT/src/dashboard_config.rs#L420-L431
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dashboard_config/accepted_domain_from_input
  - functions/LPE-CT/src/dashboard_config/normalize_accepted_domains
---

# Signature

`pub(crate) fn normalize_verification_type(value: &str) -> Result<String, ApiError>`

# Called by

- [accepted_domain_from_input](../../../../functions/LPE-CT/src/dashboard_config/accepted_domain_from_input.md)
- [normalize_accepted_domains](../../../../functions/LPE-CT/src/dashboard_config/normalize_accepted_domains.md)