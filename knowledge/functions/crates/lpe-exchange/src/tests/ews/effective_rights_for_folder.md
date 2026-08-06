---
type: Rust Function
title: effective_rights_for_folder
resource: crates/lpe-exchange/src/tests/ews.rs#L136-L150
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn effective_rights_for_folder<'a>(body: &'a str, folder_id: &str) -> &'a str`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)