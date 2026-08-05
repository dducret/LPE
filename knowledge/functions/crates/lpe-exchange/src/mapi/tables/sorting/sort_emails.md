---
type: Rust Function
title: sort_emails
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L39-L80
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/client_submit_sort_key
  - functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi/tables/tests/sent_default_view_sort_orders_by_client_submit_time
---

# Signature

`pub(in crate::mapi) fn sort_emails(rows: &mut [&JmapEmail], sort_orders: &[MapiSortOrder])`

# Calls

- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [email_sender_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name.md)
- [email_sender_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address.md)
- [display_to](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to.md)
- [client_submit_sort_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/client_submit_sort_key.md)
- [message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [format_inbox_view_descriptor_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [format_visible_inbox_first_row_projection_audit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit.md)
- [format_normal_message_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_normal_message_find_row_failure_candidates](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [expanded_categorized_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [sent_default_view_sort_orders_by_client_submit_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/sent_default_view_sort_orders_by_client_submit_time.md)