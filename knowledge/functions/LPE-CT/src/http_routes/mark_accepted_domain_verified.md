---
type: Rust Function
title: mark_accepted_domain_verified
resource: LPE-CT/src/http_routes.rs#L711-L723
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/test_accepted_domain
---

# Signature

`pub(crate) fn mark_accepted_domain_verified( domains: &mut [AcceptedDomain], domain_id: &str, ) -> bool`

# Called by

- [test_accepted_domain](../../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)