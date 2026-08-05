---
type: Rust Function
title: settings
resource: crates/lpe-admin-api/src/oidc.rs#L256-L291
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/oidc/authorization_url_contains_required_parameters
  - functions/crates/lpe-admin-api/src/oidc/generated_state_is_accepted_for_matching_origin
---

# Signature

`fn settings() -> SecuritySettings`

# Called by

- [authorization_url_contains_required_parameters](../../../../../functions/crates/lpe-admin-api/src/oidc/authorization_url_contains_required_parameters.md)
- [generated_state_is_accepted_for_matching_origin](../../../../../functions/crates/lpe-admin-api/src/oidc/generated_state_is_accepted_for_matching_origin.md)