---
type: Rust Method
title: find_account_oidc_identity
resource: crates/lpe-storage/src/auth.rs#L429-L464
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback
---

# Signature

`pub async fn find_account_oidc_identity( &self, issuer_url: &str, subject: &str, ) -> Result<Option<String>>`

# Called by

- [client_oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)