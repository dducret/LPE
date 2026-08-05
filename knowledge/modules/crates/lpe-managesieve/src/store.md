---
type: Rust Module
title: store
resource: crates/lpe-managesieve/src/store.rs#L1-L108
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-mail-auth-accountauthstore
  - external/lpe-storage-auditentryinput-sievescriptdocument-sievescriptsummary-storage
  - external/std-future-future-pin-pin
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-managesieve
---

# Contains

- [ManageSieveStore](../../../../interfaces/crates/lpe-managesieve/src/store/ManageSieveStore.md)
- [list_sieve_scripts](../../../../functions/crates/lpe-managesieve/src/store/Storage/managesievestore/list_sieve_scripts.md)
- [get_sieve_script](../../../../functions/crates/lpe-managesieve/src/store/Storage/managesievestore/get_sieve_script.md)
- [put_sieve_script](../../../../functions/crates/lpe-managesieve/src/store/Storage/managesievestore/put_sieve_script.md)
- [delete_sieve_script](../../../../functions/crates/lpe-managesieve/src/store/Storage/managesievestore/delete_sieve_script.md)
- [rename_sieve_script](../../../../functions/crates/lpe-managesieve/src/store/Storage/managesievestore/rename_sieve_script.md)
- [set_active_sieve_script](../../../../functions/crates/lpe-managesieve/src/store/Storage/managesievestore/set_active_sieve_script.md)

# Imports

- `anyhow::Result`
- `lpe_mail_auth::AccountAuthStore`
- `lpe_storage::{AuditEntryInput, SieveScriptDocument, SieveScriptSummary, Storage}`
- `std::{future::Future, pin::Pin}`
- `uuid::Uuid`

# Member of

- [lpe-managesieve](../../../../packages/crates/lpe-managesieve.md)