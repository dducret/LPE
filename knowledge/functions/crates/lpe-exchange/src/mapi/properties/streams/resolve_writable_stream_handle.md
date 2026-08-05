---
type: Rust Function
title: resolve_writable_stream_handle
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L586-L619
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_seek_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response
  - functions/crates/lpe-exchange/src/mapi/properties/tests/associated_config_missing_binary_property_opens_writable_stream
---

# Signature

`pub(in crate::mapi) fn resolve_writable_stream_handle( session: &MapiSession, requested_handle: u32, ) -> Option<u32>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [append_get_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_stream_size_response.md)
- [append_read_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response.md)
- [append_clone_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response.md)
- [append_seek_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_seek_stream_response.md)
- [append_set_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response.md)
- [append_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)
- [append_copy_to_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response.md)
- [append_commit_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response.md)
- [associated_config_missing_binary_property_opens_writable_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/associated_config_missing_binary_property_opens_writable_stream.md)