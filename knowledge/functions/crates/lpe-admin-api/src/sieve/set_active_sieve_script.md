---
type: Rust Function
title: set_active_sieve_script
resource: crates/lpe-admin-api/src/sieve.rs#L110-L133
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn set_active_sieve_script( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<SetActiveSieveScriptRequest>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)