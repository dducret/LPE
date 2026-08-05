---
type: Rust Method
title: parse_action
resource: crates/lpe-core/src/sieve.rs#L499-L542
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/consume_identifier
  - functions/crates/lpe-core/src/sieve/Parser/expect_semicolon
  - functions/crates/lpe-core/src/sieve/Parser/expect_string
  - functions/crates/lpe-core/src/sieve/Parser/consume
  - functions/crates/lpe-core/src/sieve/Parser/expect_identifier
  - functions/crates/lpe-core/src/sieve/Parser/expect_number
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_statement
---

# Signature

`fn parse_action(&mut self) -> Result<Action>`

# Calls

- [consume_identifier](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume_identifier.md)
- [expect_semicolon](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect_semicolon.md)
- [expect_string](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect_string.md)
- [consume](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume.md)
- [expect_identifier](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect_identifier.md)
- [expect_number](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect_number.md)

# Called by

- [parse_statement](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_statement.md)