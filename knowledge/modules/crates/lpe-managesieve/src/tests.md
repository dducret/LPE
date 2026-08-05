---
type: Rust Module
title: tests
resource: crates/lpe-managesieve/src/tests.rs#L1-L323
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/anyhow-anyhow
  - external/argon2-password-hash-rand-core-osrng-passwordhasher-saltstring-argon2
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-mail-auth-issue-oauth-access-token-accountauthstore
  - external/lpe-storage-accountlogin-auditentryinput-sievescriptdocument-sievescriptsummary
  - external/std-sync-arc-mutex
  - external/tokio-io-asyncreadext-asyncwriteext-net-tcplistener-tcpstream
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-managesieve
---

# Contains

- [FakeStore](../../../../classes/crates/lpe-managesieve/src/tests/FakeStore.md)
- [tenant_id](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/tenant_id.md)
- [new](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/new.md)
- [fetch_account_session](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/accountauthstore/fetch_account_session.md)
- [fetch_account_login](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/accountauthstore/fetch_account_login.md)
- [fetch_active_account_app_passwords](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/accountauthstore/fetch_active_account_app_passwords.md)
- [touch_account_app_password](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/accountauthstore/touch_account_app_password.md)
- [append_audit_event](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/accountauthstore/append_audit_event.md)
- [list_sieve_scripts](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/managesievestore/list_sieve_scripts.md)
- [get_sieve_script](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/managesievestore/get_sieve_script.md)
- [put_sieve_script](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/managesievestore/put_sieve_script.md)
- [delete_sieve_script](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/managesievestore/delete_sieve_script.md)
- [rename_sieve_script](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/managesievestore/rename_sieve_script.md)
- [set_active_sieve_script](../../../../functions/crates/lpe-managesieve/src/tests/FakeStore/managesievestore/set_active_sieve_script.md)
- [parses_putscript_request_line_with_literal_plus](../../../../functions/crates/lpe-managesieve/src/tests/parses_putscript_request_line_with_literal_plus.md)
- [managesieve_session_supports_put_list_get_and_activate](../../../../functions/crates/lpe-managesieve/src/tests/managesieve_session_supports_put_list_get_and_activate.md)
- [managesieve_accepts_xoauth2](../../../../functions/crates/lpe-managesieve/src/tests/managesieve_accepts_xoauth2.md)

# Imports

- `super::*`
- `anyhow::anyhow`
- `argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2,
}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_mail_auth::{issue_oauth_access_token, AccountAuthStore}`
- `lpe_storage::{AccountLogin, AuditEntryInput, SieveScriptDocument, SieveScriptSummary}`
- `std::sync::{Arc, Mutex}`
- `tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
}`
- `uuid::Uuid`

# Member of

- [lpe-managesieve](../../../../packages/crates/lpe-managesieve.md)