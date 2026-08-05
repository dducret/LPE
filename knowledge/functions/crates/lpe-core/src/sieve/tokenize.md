---
type: Rust Function
title: tokenize
resource: crates/lpe-core/src/sieve.rs#L290-L362
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/peek
  - functions/crates/lpe-core/src/sieve/is_identifier_start
  - functions/crates/lpe-core/src/sieve/is_identifier_char
---

# Signature

`fn tokenize(input: &str) -> Result<Vec<Token>>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [peek](../../../../../functions/crates/lpe-core/src/sieve/Parser/peek.md)
- [is_identifier_start](../../../../../functions/crates/lpe-core/src/sieve/is_identifier_start.md)
- [is_identifier_char](../../../../../functions/crates/lpe-core/src/sieve/is_identifier_char.md)