---
type: Rust Function
title: validate_sieve_script_content
resource: crates/lpe-storage/src/util.rs#L220-L229
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/put_sieve_script
---

# Signature

`pub(crate) fn validate_sieve_script_content(value: &str) -> Result<String>`

# Called by

- [put_sieve_script](../../../../../functions/crates/lpe-storage/src/admin/Storage/put_sieve_script.md)