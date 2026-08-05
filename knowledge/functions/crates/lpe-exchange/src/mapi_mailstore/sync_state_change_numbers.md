---
type: Rust Function
title: sync_state_change_numbers
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L433-L465
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
---

# Signature

`fn sync_state_change_numbers( sync_type: u8, folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], folder_versions: &[crate::mapi_store::MapiFolderVersion], ) -> Vec<u64>`

# Calls

- [mapi_folder_id_for_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [canonical_hierarchy_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number.md)
- [canonical_message_change_number_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments.md)

# Called by

- [sync_state_token_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)