---
type: Rust Function
title: get_sieve_overview
resource: crates/lpe-admin-api/src/sieve.rs#L29-L46
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn get_sieve_overview( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<SieveOverviewResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)