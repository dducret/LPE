---
type: Rust Function
title: read_request
resource: crates/lpe-managesieve/src/parse.rs#L32-L55
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-managesieve/src/service/handle_connection
---

# Signature

`pub async fn read_request<R: AsyncBufReadExt + AsyncReadExt + Unpin>( reader: &mut R, ) -> Result<Option<Request>>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_connection](../../../../../functions/crates/lpe-managesieve/src/service/handle_connection.md)