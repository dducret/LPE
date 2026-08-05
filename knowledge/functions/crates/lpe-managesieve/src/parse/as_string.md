---
type: Rust Function
title: as_string
resource: crates/lpe-managesieve/src/parse.rs#L24-L30
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-managesieve/src/auth/authenticate
  - functions/crates/lpe-managesieve/src/parse/single_string_arg
  - functions/crates/lpe-managesieve/src/service/handle_connection
---

# Signature

`pub fn as_string(argument: &Argument) -> Result<String>`

# Called by

- [authenticate](../../../../../functions/crates/lpe-managesieve/src/auth/authenticate.md)
- [single_string_arg](../../../../../functions/crates/lpe-managesieve/src/parse/single_string_arg.md)
- [handle_connection](../../../../../functions/crates/lpe-managesieve/src/service/handle_connection.md)