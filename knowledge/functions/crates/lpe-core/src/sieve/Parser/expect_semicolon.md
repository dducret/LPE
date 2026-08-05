---
type: Rust Method
title: expect_semicolon
resource: crates/lpe-core/src/sieve.rs#L590-L592
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_script
  - functions/crates/lpe-core/src/sieve/Parser/parse_action
---

# Signature

`fn expect_semicolon(&mut self) -> Result<()>`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [parse_script](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_script.md)
- [parse_action](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_action.md)