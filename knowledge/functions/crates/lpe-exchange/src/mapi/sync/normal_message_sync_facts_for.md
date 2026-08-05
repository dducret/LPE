---
type: Rust Function
title: normal_message_sync_facts_for
resource: crates/lpe-exchange/src/mapi/sync.rs#L73-L116
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi) fn normal_message_sync_facts_for( emails: &[JmapEmail], attachment_facts: &[mapi_mailstore::MessageAttachmentSyncFacts], snapshot: &MapiMailStoreSnapshot, ) -> Vec<mapi_mailstore::NormalMessageSyncFact>`

# Calls

- [message_for_canonical_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id.md)
- [canonical_message_change_number_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments.md)
- [source_key_for_uuid](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)