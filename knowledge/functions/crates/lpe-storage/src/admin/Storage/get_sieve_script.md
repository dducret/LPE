---
type: Rust Method
title: get_sieve_script
resource: crates/lpe-storage/src/admin.rs#L319-L353
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/util/validate_sieve_script_name
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn get_sieve_script( &self, account_id: Uuid, name: &str, ) -> Result<Option<SieveScriptDocument>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [validate_sieve_script_name](../../../../../../functions/crates/lpe-storage/src/util/validate_sieve_script_name.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)