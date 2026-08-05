---
type: Rust Method
title: parse_match_type
resource: crates/lpe-core/src/sieve.rs#L544-L551
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-core/src/sieve/Parser/expect_identifier
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_test
---

# Signature

`fn parse_match_type(&mut self) -> Result<MatchType>`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [expect_identifier](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect_identifier.md)

# Called by

- [parse_test](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test.md)