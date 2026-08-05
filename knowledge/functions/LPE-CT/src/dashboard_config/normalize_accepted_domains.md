---
type: Rust Function
title: normalize_accepted_domains
resource: LPE-CT/src/dashboard_config.rs#L379-L394
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/normalize_verification_type
  - functions/LPE-CT/src/dashboard_config/is_valid_domain_name
  called_by:
  - functions/LPE-CT/src/http_routes/create_accepted_domain
  - functions/LPE-CT/src/http_routes/update_accepted_domain
  - functions/LPE-CT/src/http_routes/import_accepted_domains
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) fn normalize_accepted_domains(domains: &mut Vec<AcceptedDomain>)`

# Calls

- [normalize_verification_type](../../../../functions/LPE-CT/src/dashboard_config/normalize_verification_type.md)
- [is_valid_domain_name](../../../../functions/LPE-CT/src/dashboard_config/is_valid_domain_name.md)

# Called by

- [create_accepted_domain](../../../../functions/LPE-CT/src/http_routes/create_accepted_domain.md)
- [update_accepted_domain](../../../../functions/LPE-CT/src/http_routes/update_accepted_domain.md)
- [import_accepted_domains](../../../../functions/LPE-CT/src/http_routes/import_accepted_domains.md)
- [main](../../../../functions/LPE-CT/src/main.md)