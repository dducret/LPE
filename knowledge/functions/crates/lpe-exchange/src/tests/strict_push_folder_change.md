---
type: Rust Function
title: strict_push_folder_change
resource: crates/lpe-exchange/src/tests/mod.rs#L14688-L14714
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_push_binary_property
  - functions/crates/lpe-exchange/src/tests/strict_test_xid
  - functions/crates/lpe-exchange/src/tests/strict_push_i64_property
  - functions/crates/lpe-exchange/src/tests/strict_push_utf16_property
  - functions/crates/lpe-exchange/src/tests/strict_push_i32_property
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_child_before_parent
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_empty_deletions_section
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_misaligned_boolean_lexical_size
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_missing_final_cnset
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_folder_change_after_final_state
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_duplicate_folder_property
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_final_state_missing_folder_id
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_non_replguid_final_state
---

# Signature

`fn strict_push_folder_change( bytes: &mut Vec<u8>, parent_source_key: &[u8], source_counter: u64, change_counter: u64, name: &str, boolean_width: usize, )`

# Calls

- [strict_push_binary_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_binary_property.md)
- [strict_test_xid](../../../../../functions/crates/lpe-exchange/src/tests/strict_test_xid.md)
- [strict_push_i64_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_i64_property.md)
- [strict_push_utf16_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_utf16_property.md)
- [strict_push_i32_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_i32_property.md)

# Called by

- [strict_hierarchy_decoder_rejects_child_before_parent](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_child_before_parent.md)
- [strict_hierarchy_decoder_rejects_empty_deletions_section](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_empty_deletions_section.md)
- [strict_hierarchy_decoder_rejects_misaligned_boolean_lexical_size](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_misaligned_boolean_lexical_size.md)
- [strict_hierarchy_decoder_rejects_missing_final_cnset](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_missing_final_cnset.md)
- [strict_hierarchy_decoder_rejects_folder_change_after_final_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_folder_change_after_final_state.md)
- [strict_hierarchy_decoder_rejects_duplicate_folder_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_duplicate_folder_property.md)
- [strict_hierarchy_decoder_rejects_final_state_missing_folder_id](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_final_state_missing_folder_id.md)
- [strict_hierarchy_decoder_rejects_non_replguid_final_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_non_replguid_final_state.md)