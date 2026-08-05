---
type: Rust Function
title: candidate_find_row_debug_tags
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L929-L956
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates
---

# Signature

`fn candidate_find_row_debug_tags( selected_columns: &[u32], restriction_property_tags: &[u32], ) -> Vec<u32>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [format_normal_message_find_row_failure_candidates](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates.md)