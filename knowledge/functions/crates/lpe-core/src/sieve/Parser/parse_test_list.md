---
type: Rust Method
title: parse_test_list
resource: crates/lpe-core/src/sieve.rs#L485-L497
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/parse_test
  - functions/crates/lpe-core/src/sieve/Parser/consume
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_test
---

# Signature

`fn parse_test_list(&mut self) -> Result<Vec<Test>>`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_test](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test.md)
- [consume](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume.md)

# Called by

- [parse_test](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test.md)