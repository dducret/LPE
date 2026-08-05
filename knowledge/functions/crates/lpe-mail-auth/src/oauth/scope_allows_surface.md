---
type: Rust Function
title: scope_allows_surface
resource: crates/lpe-mail-auth/src/oauth.rs#L197-L202
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token
---

# Signature

`pub(crate) fn scope_allows_surface(scope: &str, surface: &str) -> bool`

# Called by

- [authenticate_bearer_access_token](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token.md)