---
type: Rust Method
title: consume
resource: crates/lpe-core/src/sieve.rs#L612-L620
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/peek
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_block
  - functions/crates/lpe-core/src/sieve/Parser/parse_test_list
  - functions/crates/lpe-core/src/sieve/Parser/parse_action
  - functions/crates/lpe-core/src/sieve/Parser/parse_string_list
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn consume(&mut self, expected: &Token) -> bool`

# Calls

- [peek](../../../../../../functions/crates/lpe-core/src/sieve/Parser/peek.md)

# Called by

- [parse_block](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_block.md)
- [parse_test_list](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test_list.md)
- [parse_action](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_action.md)
- [parse_string_list](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_string_list.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)