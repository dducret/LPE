---
type: Rust Module
title: bootstrap
resource: crates/lpe-admin-api/src/bootstrap.rs#L1-L166
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-hash-password-types-bootstrapadminrequest-bootstrapadminresponse-min-admin-password-len-min-integration-secret-len
  - external/lpe-storage-admincredentialinput-auditentryinput-newserveradministrator-storage
  - external/std-env
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [bootstrap_admin_request_from_env](../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env.md)
- [bootstrap_admin_request_from_env_or_defaults](../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env_or_defaults.md)
- [bootstrap_admin](../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin.md)
- [integration_shared_secret](../../../../functions/crates/lpe-admin-api/src/bootstrap/integration_shared_secret.md)
- [required_env](../../../../functions/crates/lpe-admin-api/src/bootstrap/required_env.md)
- [validate_bootstrap_admin_request](../../../../functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request.md)
- [validate_admin_password](../../../../functions/crates/lpe-admin-api/src/bootstrap/validate_admin_password.md)
- [validate_shared_secret](../../../../functions/crates/lpe-admin-api/src/bootstrap/validate_shared_secret.md)
- [is_known_weak_secret](../../../../functions/crates/lpe-admin-api/src/bootstrap/is_known_weak_secret.md)

# Imports

- `crate::{
    hash_password,
    types::{BootstrapAdminRequest, BootstrapAdminResponse},
    MIN_ADMIN_PASSWORD_LEN, MIN_INTEGRATION_SECRET_LEN,
}`
- `lpe_storage::{AdminCredentialInput, AuditEntryInput, NewServerAdministrator, Storage}`
- `std::env`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)