---
type: Rust Function
title: append_public_folder_content_sync_with_state
resource: crates/lpe-exchange/src/tests/mapi_over_http/public_folders.rs#L3-L41
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_read_state
---

# Signature

`fn append_public_folder_content_sync_with_state( rops: &mut Vec<u8>, input: u8, output: u8, synchronization_flags: u16, state_properties: &[(u32, Vec<u8>)], )`

# Called by

- [mapi_over_http_public_folder_content_sync_exports_canonical_items](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_items.md)
- [mapi_over_http_public_folder_content_sync_exports_canonical_read_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_read_state.md)