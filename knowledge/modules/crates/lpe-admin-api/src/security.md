---
type: Rust Module
title: security
resource: crates/lpe-admin-api/src/security.rs#L1-L57
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/argon2-password-hash-passwordhash-passwordhasher-passwordverifier-saltstring-argon2
  - external/lpe-mail-auth-default-oauth-access-token-seconds
  - external/std-env
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [generate_app_password_secret](../../../../functions/crates/lpe-admin-api/src/security/generate_app_password_secret.md)
- [admin_session_minutes](../../../../functions/crates/lpe-admin-api/src/security/admin_session_minutes.md)
- [client_session_minutes](../../../../functions/crates/lpe-admin-api/src/security/client_session_minutes.md)
- [client_oauth_access_token_seconds](../../../../functions/crates/lpe-admin-api/src/security/client_oauth_access_token_seconds.md)
- [hash_password](../../../../functions/crates/lpe-admin-api/src/security/hash_password.md)
- [verify_password](../../../../functions/crates/lpe-admin-api/src/security/verify_password.md)

# Imports

- `argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
}`
- `lpe_mail_auth::DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS`
- `std::env`
- `uuid::Uuid`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)