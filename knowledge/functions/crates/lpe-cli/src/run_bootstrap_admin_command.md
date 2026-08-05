---
type: Rust Function
title: run_bootstrap_admin_command
resource: crates/lpe-cli/src/main.rs#L137-L148
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin
  called_by:
  - functions/crates/lpe-cli/src/main
---

# Signature

`async fn run_bootstrap_admin_command() -> Result<()>`

# Calls

- [connect](../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [bootstrap_admin_request_from_env](../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env.md)
- [bootstrap_admin](../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin.md)

# Called by

- [main](../../../../functions/crates/lpe-cli/src/main.md)