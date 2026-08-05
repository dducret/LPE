---
type: Rust Function
title: validate_sieve_script_name
resource: crates/lpe-storage/src/util.rs#L206-L218
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/get_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/put_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script
---

# Signature

`pub(crate) fn validate_sieve_script_name(value: &str) -> Result<String>`

# Called by

- [get_sieve_script](../../../../../functions/crates/lpe-storage/src/admin/Storage/get_sieve_script.md)
- [put_sieve_script](../../../../../functions/crates/lpe-storage/src/admin/Storage/put_sieve_script.md)
- [delete_sieve_script](../../../../../functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script.md)
- [rename_sieve_script](../../../../../functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script.md)
- [set_active_sieve_script](../../../../../functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script.md)