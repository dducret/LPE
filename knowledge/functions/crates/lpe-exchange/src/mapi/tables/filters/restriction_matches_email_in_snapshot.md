---
type: Rust Function
title: restriction_matches_email_in_snapshot
resource: crates/lpe-exchange/src/mapi/tables/filters.rs#L3-L16
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_matches
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_conversation_member_in_snapshot
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(super) fn restriction_matches_email_in_snapshot( restriction: Option<&MapiRestriction>, email: &JmapEmail, folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> bool`

# Calls

- [restriction_matches_email_with_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [expanded_categorized_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows.md)
- [deleted_items_content_row_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_matches.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [restriction_matches_conversation_member_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_conversation_member_in_snapshot.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)