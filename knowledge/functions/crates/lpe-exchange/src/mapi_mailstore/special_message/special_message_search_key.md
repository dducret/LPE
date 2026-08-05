---
type: Rust Function
title: special_message_search_key
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L103-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_binary_property
  - functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`pub(super) fn special_message_search_key(object: &SpecialMessageSyncFact) -> Vec<u8>`

# Calls

- [special_message_binary_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_binary_property.md)
- [generated_message_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [write_fast_transfer_special_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)