---
type: Rust Function
title: download_change_facts
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L40-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection
---

# Signature

`pub(crate) fn download_change_facts( sync_type: u8, sync_flags: u16, folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], special_objects: &[SpecialMessageSyncFact], folder_versions: &[crate::mapi_store::MapiFolderVersion], ) -> Vec<DownloadChangeFact>`

# Calls

- [download_change_facts_with_normal_message_sync_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts.md)

# Called by

- [fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given.md)
- [normal_message_no_foreign_identifiers_uses_local_source_key_for_selection](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection.md)