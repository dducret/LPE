---
type: Rust Function
title: final_sync_state_stream
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L477-L488
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_content_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/final_sync_state_separates_object_idset_from_change_cnset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes
---

# Signature

`pub(crate) fn final_sync_state_stream( sync_type: u8, object_ids: &[u64], change_numbers: &[u64], ) -> Vec<u8>`

# Calls

- [final_content_sync_state_stream](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_content_sync_state_stream.md)
- [final_sync_state_stream_with_cnsets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets.md)

# Called by

- [initial_sync_state_stream](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [sync_state_token_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)
- [final_sync_state_separates_object_idset_from_change_cnset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/final_sync_state_separates_object_idset_from_change_cnset.md)
- [scoped_final_sync_state_uses_the_durable_inbox_counter](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter.md)
- [hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes.md)