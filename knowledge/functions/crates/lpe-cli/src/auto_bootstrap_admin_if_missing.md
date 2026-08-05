---
type: Rust Function
title: auto_bootstrap_admin_if_missing
resource: crates/lpe-cli/src/main.rs#L119-L135
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/auth/Storage/has_admin_bootstrap_state
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env_or_defaults
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin
  called_by:
  - functions/crates/lpe-cli/src/main
---

# Signature

`async fn auto_bootstrap_admin_if_missing(storage: &Storage) -> Result<()>`

# Calls

- [has_admin_bootstrap_state](../../../../functions/crates/lpe-storage/src/auth/Storage/has_admin_bootstrap_state.md)
- [bootstrap_admin_request_from_env_or_defaults](../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env_or_defaults.md)
- [bootstrap_admin](../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin.md)

# Called by

- [main](../../../../functions/crates/lpe-cli/src/main.md)