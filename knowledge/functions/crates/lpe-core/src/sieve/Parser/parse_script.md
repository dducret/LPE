---
type: Rust Method
title: parse_script
resource: crates/lpe-core/src/sieve.rs#L378-L395
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/consume_identifier
  - functions/crates/lpe-core/src/sieve/Parser/parse_string_list
  - functions/crates/lpe-core/src/sieve/Parser/expect_semicolon
  - functions/crates/lpe-core/src/sieve/validate_requirements
  - functions/crates/lpe-core/src/sieve/Parser/is_eof
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/parse_statement
---

# Signature

`fn parse_script(&mut self) -> Result<Script>`

# Calls

- [consume_identifier](../../../../../../functions/crates/lpe-core/src/sieve/Parser/consume_identifier.md)
- [parse_string_list](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_string_list.md)
- [expect_semicolon](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect_semicolon.md)
- [validate_requirements](../../../../../../functions/crates/lpe-core/src/sieve/validate_requirements.md)
- [is_eof](../../../../../../functions/crates/lpe-core/src/sieve/Parser/is_eof.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_statement](../../../../../../functions/crates/lpe-core/src/sieve/Parser/parse_statement.md)