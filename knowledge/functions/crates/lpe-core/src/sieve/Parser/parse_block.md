---
type: Rust Method
title: parse_block
resource: crates/lpe-core/src/sieve.rs#L424-L434
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-core/src/sieve/Parser/consume
  - functions/crates/lpe-core/src/sieve/Parser/is_eof
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/parse_statement
  called_by:
  - functions/crates/lpe-core/src/sieve/Parser/parse_if
---

# Signature

`fn parse_block(&mut self) -> Result<Vec<Statement>>`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [consume](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume.md)
- [is_eof](../../../../../../functions/crates/lpe-core/src/sieve/Parser/is_eof.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_statement](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_statement.md)

# Called by

- [parse_if](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_if.md)