---
type: Rust Function
title: fast_transfer_message_content_buffer_with_attachments
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L801-L818
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties
---

# Signature

`pub(crate) fn fast_transfer_message_content_buffer_with_attachments( email: &JmapEmail, attachment_facts: &[MessageAttachmentSyncFacts], durable_identity: Option<&crate::store::MapiIdentityRecord>, property_filter: FastTransferDirectPropertyFilter<'_>, message_children: FastTransferMessageChildren, ) -> Vec<u8>`

# Calls

- [write_fast_transfer_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [fast_transfer_copy_properties_filters_message_identity_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties.md)
- [direct_fast_transfer_uses_persisted_normal_message_identity_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties.md)