---
type: Rust Method
title: consume_identifier
resource: crates/lpe-core/src/sieve.rs#L602-L610
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/peek
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_script
  - functions/crates/lpe-core/src/sieve/Parser/parse_statement
  - functions/crates/lpe-core/src/sieve/Parser/parse_if
  - functions/crates/lpe-core/src/sieve/Parser/parse_test
  - functions/crates/lpe-core/src/sieve/Parser/parse_action
---

# Signature

`fn consume_identifier(&mut self, expected: &str) -> bool`

# Calls

- [peek](../../../../../../functions/crates/lpe-core/src/sieve/Parser/peek.md)

# Called by

- [parse_script](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_script.md)
- [parse_statement](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_statement.md)
- [parse_if](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_if.md)
- [parse_test](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test.md)
- [parse_action](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_action.md)