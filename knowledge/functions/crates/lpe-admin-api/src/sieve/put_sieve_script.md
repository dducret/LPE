---
type: Rust Function
title: put_sieve_script
resource: crates/lpe-admin-api/src/sieve.rs#L62-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn put_sieve_script( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertSieveScriptRequest>, ) -> ApiResult<SieveScriptDocument>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)