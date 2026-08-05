---
type: Rust Function
title: handle_havespace
resource: crates/lpe-managesieve/src/service.rs#L210-L227
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-managesieve/src/service/handle_connection
---

# Signature

`async fn handle_havespace<W: AsyncWriteExt + Unpin>( writer: &mut W, arguments: &[Argument], ) -> Result<()>`

# Called by

- [handle_connection](../../../../../functions/crates/lpe-managesieve/src/service/handle_connection.md)