---
type: Rust Function
title: hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L2654-L2694
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
---

# Signature

`fn hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)