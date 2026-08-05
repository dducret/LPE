---
type: Rust Method
title: expect_identifier
resource: crates/lpe-core/src/sieve.rs#L576-L581
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_action
  - functions/crates/lpe-core/src/sieve/Parser/parse_match_type
---

# Signature

`fn expect_identifier(&mut self) -> Result<String>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_action](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_action.md)
- [parse_match_type](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_match_type.md)