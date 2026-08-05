---
type: Rust Function
title: rename_sieve_script
resource: crates/lpe-admin-api/src/sieve.rs#L87-L108
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn rename_sieve_script( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<RenameSieveScriptRequest>, ) -> ApiResult<lpe_storage::SieveScriptSummary>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)