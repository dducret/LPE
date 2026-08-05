---
type: Rust Method
title: is_inbox_associated_config_stream_handle
resource: crates/lpe-exchange/src/mapi/session.rs#L384-L387
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response
---

# Signature

`pub(in crate::mapi) fn is_inbox_associated_config_stream_handle(&self, handle: u32) -> bool`

# Called by

- [append_read_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response.md)
- [append_set_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response.md)
- [append_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)
- [append_commit_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response.md)