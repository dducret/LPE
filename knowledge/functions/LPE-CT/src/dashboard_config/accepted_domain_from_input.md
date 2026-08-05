---
type: Rust Function
title: accepted_domain_from_input
resource: LPE-CT/src/dashboard_config.rs#L347-L377
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/is_valid_domain_name
  - functions/LPE-CT/src/dashboard_config/normalize_verification_type
  called_by:
  - functions/LPE-CT/src/http_routes/create_accepted_domain
  - functions/LPE-CT/src/http_routes/update_accepted_domain
  - functions/LPE-CT/src/http_routes/import_accepted_domains
---

# Signature

`pub(crate) fn accepted_domain_from_input( input: AcceptedDomainInput, existing_id: Option<String>, ) -> Result<AcceptedDomain, ApiError>`

# Calls

- [is_valid_domain_name](../../../../functions/LPE-CT/src/dashboard_config/is_valid_domain_name.md)
- [normalize_verification_type](../../../../functions/LPE-CT/src/dashboard_config/normalize_verification_type.md)

# Called by

- [create_accepted_domain](../../../../functions/LPE-CT/src/http_routes/create_accepted_domain.md)
- [update_accepted_domain](../../../../functions/LPE-CT/src/http_routes/update_accepted_domain.md)
- [import_accepted_domains](../../../../functions/LPE-CT/src/http_routes/import_accepted_domains.md)