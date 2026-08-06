---
type: Rust Function
title: append_rop_outlook_content_sync_manifest_get_buffer_with_state
resource: crates/lpe-exchange/src/tests/mod.rs#L15280-L15332
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_request_rops
---

# Signature

`fn append_rop_outlook_content_sync_manifest_get_buffer_with_state( rops: &mut Vec<u8>, input: u8, output: u8, buffer_size: u16, state_properties: &[(u32, Vec<u8>)], )`

# Called by

- [mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state.md)
- [outlook_content_sync_request_rops](../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_request_rops.md)