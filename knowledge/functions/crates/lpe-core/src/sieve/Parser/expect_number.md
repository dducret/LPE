---
type: Rust Method
title: expect_number
resource: crates/lpe-core/src/sieve.rs#L583-L588
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_action
---

# Signature

`fn expect_number(&mut self) -> Result<u32>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_action](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_action.md)