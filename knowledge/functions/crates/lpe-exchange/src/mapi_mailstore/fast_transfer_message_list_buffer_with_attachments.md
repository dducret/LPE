---
type: Rust Function
title: fast_transfer_message_list_buffer_with_attachments
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L773-L799
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers
---

# Signature

`pub(crate) fn fast_transfer_message_list_buffer_with_attachments( emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], ) -> Vec<u8>`

# Calls

- [write_fast_transfer_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content.md)

# Called by

- [append_fast_transfer_source_copy_messages_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response.md)
- [write_fast_transfer_folder_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content.md)
- [microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers.md)