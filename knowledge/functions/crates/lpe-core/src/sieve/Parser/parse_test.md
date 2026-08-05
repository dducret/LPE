---
type: Rust Method
title: parse_test
resource: crates/lpe-core/src/sieve.rs#L436-L483
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/consume_identifier
  - functions/crates/lpe-core/src/sieve/Parser/parse_test_list
  - functions/crates/lpe-core/src/sieve/Parser/parse_match_type
  - functions/crates/lpe-core/src/sieve/Parser/parse_string_list
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_if
  - functions/crates/lpe-core/src/sieve/Parser/parse_test_list
---

# Signature

`fn parse_test(&mut self) -> Result<Test>`

# Calls

- [consume_identifier](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume_identifier.md)
- [parse_test_list](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test_list.md)
- [parse_match_type](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_match_type.md)
- [parse_string_list](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_string_list.md)

# Called by

- [parse_if](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_if.md)
- [parse_test_list](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test_list.md)