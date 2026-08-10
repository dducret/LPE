---
type: Rust Function
title: sync_state_token_with_special_objects_and_normal_message_facts
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L435-L540
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_normal
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_source_key_for_fact
  - functions/crates/lpe-exchange/src/mapi_mailstore/default_content_sync_includes_associated
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_associated
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_from_raw_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn sync_state_token_with_special_objects_and_normal_message_facts( sync_type: u8, sync_flags: u16, folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], normal_message_facts: &[NormalMessageSyncFact], special_objects: &[SpecialMessageSyncFact], folder_versions: &[crate::mapi_store::MapiFolderVersion], ) -> Vec<u8>`

# Calls

- [final_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream.md)
- [sync_state_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_object_ids.md)
- [sync_state_change_numbers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers.md)
- [content_sync_includes_normal](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_normal.md)
- [normal_message_sync_fact_for](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for.md)
- [normal_message_sync_source_key_for_fact](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_source_key_for_fact.md)
- [default_content_sync_includes_associated](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/default_content_sync_includes_associated.md)
- [content_sync_includes_associated](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_associated.md)
- [special_message_sync_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_source_key.md)
- [special_message_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_number.md)
- [sync_state_stream_from_raw_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_from_raw_properties.md)
- [replguid_idset_from_source_keys](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/replguid_idset_from_source_keys.md)
- [replguid_idset_from_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)