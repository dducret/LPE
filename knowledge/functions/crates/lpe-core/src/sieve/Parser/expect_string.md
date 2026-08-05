---
type: Rust Method
title: expect_string
resource: crates/lpe-core/src/sieve.rs#L569-L574
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_action
  - functions/crates/lpe-core/src/sieve/Parser/parse_string_list
---

# Signature

`fn expect_string(&mut self) -> Result<String>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_action](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_action.md)
- [parse_string_list](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_string_list.md)