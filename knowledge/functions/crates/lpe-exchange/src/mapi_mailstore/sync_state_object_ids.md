---
type: Rust Function
title: sync_state_object_ids
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L416-L434
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
---

# Signature

`fn sync_state_object_ids( sync_type: u8, folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> Vec<u64>`

# Calls

- [mapi_folder_id_for_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [sync_state_token_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)