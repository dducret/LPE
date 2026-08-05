---
type: Rust Function
title: fast_transfer_top_folder_buffer_with_attachments
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L850-L875
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers
---

# Signature

`pub(crate) fn fast_transfer_top_folder_buffer_with_attachments( folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], ) -> Vec<u8>`

# Calls

- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [mapi_folder_id_for_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [write_fast_transfer_folder_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers.md)
- [microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers.md)