---
type: Rust Function
title: strict_push_final_hierarchy_state
resource: crates/lpe-exchange/src/tests/mod.rs#L14711-L14725
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_push_binary_property
  - functions/crates/lpe-exchange/src/tests/strict_test_replguid_globset
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_child_before_parent
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_accepts_deletion_only_delta
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_empty_deletions_section
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_misaligned_boolean_lexical_size
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_duplicate_folder_property
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_message_change_in_hierarchy_stream
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_final_state_missing_folder_id
---

# Signature

`fn strict_push_final_hierarchy_state(bytes: &mut Vec<u8>, source_ids: &[u64], changes: &[u64])`

# Calls

- [strict_push_binary_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_binary_property.md)
- [strict_test_replguid_globset](../../../../../functions/crates/lpe-exchange/src/tests/strict_test_replguid_globset.md)

# Called by

- [strict_hierarchy_decoder_rejects_child_before_parent](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_child_before_parent.md)
- [strict_hierarchy_decoder_accepts_deletion_only_delta](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_accepts_deletion_only_delta.md)
- [strict_hierarchy_decoder_rejects_empty_deletions_section](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_empty_deletions_section.md)
- [strict_hierarchy_decoder_rejects_misaligned_boolean_lexical_size](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_misaligned_boolean_lexical_size.md)
- [strict_hierarchy_decoder_rejects_duplicate_folder_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_duplicate_folder_property.md)
- [strict_hierarchy_decoder_rejects_message_change_in_hierarchy_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_message_change_in_hierarchy_stream.md)
- [strict_hierarchy_decoder_rejects_final_state_missing_folder_id](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_final_state_missing_folder_id.md)