---
type: Rust Function
title: mapi_folder_display_name
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L136-L143
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn mapi_folder_display_name(mailbox: &JmapMailbox) -> &str`

# Calls

- [virtual_special_folder_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata.md)
- [mapi_folder_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)

# Called by

- [write_fast_transfer_folder_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties.md)
- [fast_transfer_child_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)