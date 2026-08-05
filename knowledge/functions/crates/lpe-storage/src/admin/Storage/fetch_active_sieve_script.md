---
type: Rust Method
title: fetch_active_sieve_script
resource: crates/lpe-storage/src/admin.rs#L736-L767
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn fetch_active_sieve_script( &self, account_id: Uuid, ) -> Result<Option<SieveScriptDocument>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)