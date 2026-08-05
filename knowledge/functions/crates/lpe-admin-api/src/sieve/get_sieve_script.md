---
type: Rust Function
title: get_sieve_script
resource: crates/lpe-admin-api/src/sieve.rs#L48-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn get_sieve_script( State(storage): State<Storage>, headers: HeaderMap, AxumPath(name): AxumPath<String>, ) -> ApiResult<SieveScriptDocument>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)