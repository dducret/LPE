---
type: Rust Function
title: canonical_message_flags
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1026-L1036
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags_for_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn canonical_message_flags(email: &JmapEmail) -> u32`

# Calls

- [canonical_message_flags_for_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags_for_state.md)

# Called by

- [message_flags](../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags.md)
- [fast_transfer_manifest_buffer_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)