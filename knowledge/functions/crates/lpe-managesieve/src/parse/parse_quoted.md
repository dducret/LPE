---
type: Rust Function
title: parse_quoted
resource: crates/lpe-managesieve/src/parse.rs#L100-L122
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-managesieve/src/parse/parse_request_line
---

# Signature

`fn parse_quoted<I>(chars: &mut std::iter::Peekable<I>) -> Result<String> where I: Iterator<Item = char>,`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_request_line](../../../../../functions/crates/lpe-managesieve/src/parse/parse_request_line.md)