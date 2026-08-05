---
type: Rust Function
title: parse_literal_marker
resource: crates/lpe-managesieve/src/parse.rs#L124-L144
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-core/src/sieve/Parser/peek
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-managesieve/src/parse/parse_request_line
---

# Signature

`fn parse_literal_marker<I>(chars: &mut std::iter::Peekable<I>) -> Result<usize> where I: Iterator<Item = char>,`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [peek](../../../../../functions/crates/lpe-core/src/sieve/Parser/peek.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_request_line](../../../../../functions/crates/lpe-managesieve/src/parse/parse_request_line.md)