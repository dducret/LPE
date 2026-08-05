---
type: Rust Function
title: write_capability
resource: crates/lpe-managesieve/src/service.rs#L194-L208
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-managesieve/src/service/handle_connection
---

# Signature

`async fn write_capability<W: AsyncWriteExt + Unpin>(writer: &mut W) -> Result<()>`

# Called by

- [handle_connection](../../../../../functions/crates/lpe-managesieve/src/service/handle_connection.md)