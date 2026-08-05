---
type: Rust Module
title: management_auth
resource: LPE-CT/src/management_auth.rs#L1-L194
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/argon2-password-hash-passwordhash-passwordhasher-passwordverifier-saltstring-argon2
  member_of:
  - packages/LPE-CT
---

# Contains

- [ApiError](../../../classes/LPE-CT/src/management_auth/ApiError.md)
- [new](../../../functions/LPE-CT/src/management_auth/ApiError/new.md)
- [from](../../../functions/LPE-CT/src/management_auth/ApiError/from-anyhow-error/from.md)
- [into_response](../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)
- [require_management_admin](../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [bearer_token](../../../functions/LPE-CT/src/management_auth/bearer_token.md)
- [require_integration_request](../../../functions/LPE-CT/src/management_auth/require_integration_request.md)
- [required_header](../../../functions/LPE-CT/src/management_auth/required_header.md)
- [ensure_not_replayed](../../../functions/LPE-CT/src/management_auth/ensure_not_replayed.md)
- [integration_auth_api_error](../../../functions/LPE-CT/src/management_auth/integration_auth_api_error.md)
- [integration_shared_secret](../../../functions/LPE-CT/src/management_auth/integration_shared_secret.md)
- [hash_password](../../../functions/LPE-CT/src/management_auth/hash_password.md)
- [verify_password](../../../functions/LPE-CT/src/management_auth/verify_password.md)
- [is_known_weak_secret](../../../functions/LPE-CT/src/management_auth/is_known_weak_secret.md)

# Imports

- `super::*`
- `argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
}`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)