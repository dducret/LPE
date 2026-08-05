---
type: Rust Method
title: parse_if
resource: crates/lpe-core/src/sieve.rs#L404-L422
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/parse_test
  - functions/crates/lpe-core/src/sieve/Parser/parse_block
  - functions/crates/lpe-core/src/sieve/Parser/consume_identifier
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_statement
---

# Signature

`fn parse_if(&mut self) -> Result<Statement>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_test](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_test.md)
- [parse_block](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_block.md)
- [consume_identifier](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume_identifier.md)

# Called by

- [parse_statement](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_statement.md)