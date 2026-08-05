---
type: Rust Method
title: parse_string_list
resource: crates/lpe-core/src/sieve.rs#L553-L567
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/consume
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/expect_string
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_script
  - functions/crates/lpe-core/src/sieve/Parser/parse_test
---

# Signature

`fn parse_string_list(&mut self) -> Result<Vec<String>>`

# Calls

- [consume](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [expect_string](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect_string.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [parse_script](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_script.md)
- [parse_test](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test.md)