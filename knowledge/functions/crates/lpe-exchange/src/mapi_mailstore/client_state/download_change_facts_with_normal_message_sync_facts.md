---
type: Rust Function
title: download_change_facts_with_normal_message_sync_facts
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L63-L135
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_normal
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_source_key_for_fact
  - functions/crates/lpe-exchange/src/mapi_mailstore/default_content_sync_includes_associated
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_associated
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_source_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync
---

# Signature

`pub(crate) fn download_change_facts_with_normal_message_sync_facts( sync_type: u8, sync_flags: u16, folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], normal_message_facts: &[NormalMessageSyncFact], special_objects: &[SpecialMessageSyncFact], folder_versions: &[crate::mapi_store::MapiFolderVersion], ) -> Vec<DownloadChangeFact>`

# Calls

- [mapi_folder_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [canonical_hierarchy_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [content_sync_includes_normal](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_normal.md)
- [normal_message_sync_fact_for](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for.md)
- [normal_message_sync_source_key_for_fact](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_source_key_for_fact.md)
- [default_content_sync_includes_associated](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/default_content_sync_includes_associated.md)
- [content_sync_includes_associated](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_associated.md)
- [special_message_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_number.md)
- [special_message_sync_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_source_key.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [download_change_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts.md)
- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync.md)