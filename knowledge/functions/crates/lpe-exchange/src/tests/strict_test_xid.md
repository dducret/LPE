---
type: Rust Function
title: strict_test_xid
resource: crates/lpe-exchange/src/tests/mod.rs#L13600-L13604
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_push_folder_change
  - functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_child_before_parent
---

# Signature

`fn strict_test_xid(counter: u64) -> Vec<u8>`

# Called by

- [strict_push_folder_change](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_folder_change.md)
- [strict_content_decoder_accepts_imported_change_key_with_server_change_number](../../../../../functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number.md)
- [strict_hierarchy_decoder_rejects_child_before_parent](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_child_before_parent.md)