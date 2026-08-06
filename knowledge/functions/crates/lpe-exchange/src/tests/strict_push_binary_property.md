---
type: Rust Function
title: strict_push_binary_property
resource: crates/lpe-exchange/src/tests/mod.rs#L14654-L14658
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_push_folder_change
  - functions/crates/lpe-exchange/src/tests/strict_push_final_hierarchy_state
  - functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_accepts_deletion_only_delta
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_missing_final_cnset
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_folder_change_after_final_state
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_non_replguid_final_state
---

# Signature

`fn strict_push_binary_property(bytes: &mut Vec<u8>, tag: u32, value: &[u8])`

# Called by

- [strict_push_folder_change](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_folder_change.md)
- [strict_push_final_hierarchy_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_final_hierarchy_state.md)
- [strict_content_decoder_accepts_imported_change_key_with_server_change_number](../../../../../functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number.md)
- [strict_hierarchy_decoder_accepts_deletion_only_delta](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_accepts_deletion_only_delta.md)
- [strict_hierarchy_decoder_rejects_missing_final_cnset](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_missing_final_cnset.md)
- [strict_hierarchy_decoder_rejects_folder_change_after_final_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_folder_change_after_final_state.md)
- [strict_hierarchy_decoder_rejects_non_replguid_final_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_non_replguid_final_state.md)