---
type: Rust Function
title: parse_request_line
resource: crates/lpe-managesieve/src/parse.rs#L57-L77
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-managesieve/src/parse/parse_atom
  - functions/crates/lpe-managesieve/src/parse/skip_ws
  - functions/crates/lpe-core/src/sieve/Parser/peek
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-managesieve/src/parse/parse_quoted
  - functions/crates/lpe-managesieve/src/parse/parse_literal_marker
---

# Signature

`pub fn parse_request_line(input: &str) -> Result<(String, Vec<Argument>, Option<usize>)>`

# Calls

- [parse_atom](../../../../../functions/crates/lpe-managesieve/src/parse/parse_atom.md)
- [skip_ws](../../../../../functions/crates/lpe-managesieve/src/parse/skip_ws.md)
- [peek](../../../../../functions/crates/lpe-core/src/sieve/Parser/peek.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_quoted](../../../../../functions/crates/lpe-managesieve/src/parse/parse_quoted.md)
- [parse_literal_marker](../../../../../functions/crates/lpe-managesieve/src/parse/parse_literal_marker.md)