---
type: Rust Function
title: single_string_arg
resource: crates/lpe-managesieve/src/parse.rs#L17-L22
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-managesieve/src/parse/as_string
  called_by:
  - functions/crates/lpe-managesieve/src/service/handle_connection
---

# Signature

`pub fn single_string_arg(arguments: &[Argument]) -> Result<String>`

# Calls

- [as_string](../../../../../functions/crates/lpe-managesieve/src/parse/as_string.md)

# Called by

- [handle_connection](../../../../../functions/crates/lpe-managesieve/src/service/handle_connection.md)