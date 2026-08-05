---
type: Rust Function
title: parse_atom
resource: crates/lpe-managesieve/src/parse.rs#L79-L98
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/peek
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-managesieve/src/parse/parse_request_line
---

# Signature

`fn parse_atom<I>(chars: &mut std::iter::Peekable<I>) -> Result<String> where I: Iterator<Item = char>,`

# Calls

- [peek](../../../../../functions/crates/lpe-core/src/sieve/Parser/peek.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_request_line](../../../../../functions/crates/lpe-managesieve/src/parse/parse_request_line.md)