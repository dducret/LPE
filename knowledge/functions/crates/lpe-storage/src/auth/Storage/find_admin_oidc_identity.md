---
type: Rust Method
title: find_admin_oidc_identity
resource: crates/lpe-storage/src/auth.rs#L194-L227
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
---

# Signature

`pub async fn find_admin_oidc_identity( &self, issuer_url: &str, subject: &str, ) -> Result<Option<String>>`

# Called by

- [oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)