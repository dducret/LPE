---
type: Rust Function
title: handle_connection
resource: crates/lpe-managesieve/src/service.rs#L39-L192
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-managesieve/src/parse/read_request
  - functions/crates/lpe-managesieve/src/service/write_capability
  - functions/crates/lpe-managesieve/src/service/handle_havespace
  - functions/crates/lpe-managesieve/src/parse/single_string_arg
  - functions/crates/lpe-managesieve/src/parse/as_string
---

# Signature

`async fn handle_connection<S: ManageSieveStore>(store: S, stream: TcpStream) -> Result<()>`

# Calls

- [read_request](../../../../../functions/crates/lpe-managesieve/src/parse/read_request.md)
- [write_capability](../../../../../functions/crates/lpe-managesieve/src/service/write_capability.md)
- [handle_havespace](../../../../../functions/crates/lpe-managesieve/src/service/handle_havespace.md)
- [single_string_arg](../../../../../functions/crates/lpe-managesieve/src/parse/single_string_arg.md)
- [as_string](../../../../../functions/crates/lpe-managesieve/src/parse/as_string.md)