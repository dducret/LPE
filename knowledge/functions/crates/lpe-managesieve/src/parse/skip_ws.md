---
type: Rust Function
title: skip_ws
resource: crates/lpe-managesieve/src/parse.rs#L146-L153
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-managesieve/src/parse/parse_request_line
---

# Signature

`fn skip_ws<I>(chars: &mut std::iter::Peekable<I>) where I: Iterator<Item = char>,`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_request_line](../../../../../functions/crates/lpe-managesieve/src/parse/parse_request_line.md)