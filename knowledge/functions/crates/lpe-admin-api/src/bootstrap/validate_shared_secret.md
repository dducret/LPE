---
type: Rust Function
title: validate_shared_secret
resource: crates/lpe-admin-api/src/bootstrap.rs#L140-L149
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/bootstrap/integration_shared_secret
---

# Signature

`fn validate_shared_secret(name: &str, secret: &str) -> anyhow::Result<()>`

# Called by

- [integration_shared_secret](../../../../../functions/crates/lpe-admin-api/src/bootstrap/integration_shared_secret.md)