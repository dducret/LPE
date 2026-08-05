---
type: Rust Function
title: assert_variable_property_present
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L4223-L4230
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_embedded_message_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/outlook_fai_copyto_generates_a_mapiuid_search_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset
---

# Signature

`fn assert_variable_property_present(buffer: &[u8], property_tag: u32, value: &[u8])`

# Called by

- [microsoft_oxcfxics_content_sync_uses_recipient_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers.md)
- [microsoft_oxcfxics_content_sync_uses_attachment_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers.md)
- [microsoft_oxcfxics_content_sync_uses_embedded_message_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_embedded_message_markers.md)
- [microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers.md)
- [microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root.md)
- [outlook_fai_copyto_generates_a_mapiuid_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/outlook_fai_copyto_generates_a_mapiuid_search_key.md)
- [microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers.md)
- [microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers.md)
- [hierarchy_download_emits_explicit_tombstone_absent_from_client_idset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset.md)