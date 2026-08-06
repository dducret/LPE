---
type: Rust Function
title: serialize_message_row
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L97-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_durable_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/tests/bcc_projections_only_expose_drafts_and_sent_items
  - functions/crates/lpe-exchange/src/mapi/tables/tests/message_row_projects_containing_folder_ids
  - functions/crates/lpe-exchange/src/mapi/tables/tests/draft_message_row_projects_mf_unsent_from_canonical_mailbox_state
  - functions/crates/lpe-exchange/src/mapi/tables/tests/message_row_client_submit_time_falls_back_to_received_time
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_outlook_inbox_view_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_microsoft_view_descriptor_string8_columns
---

# Signature

`pub(in crate::mapi) fn serialize_message_row(email: &JmapEmail, columns: &[u32]) -> Vec<u8>`

# Calls

- [serialize_message_row_with_durable_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_durable_identity.md)

# Called by

- [format_visible_inbox_first_row_projection_audit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit.md)
- [format_normal_message_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [bcc_projections_only_expose_drafts_and_sent_items](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/bcc_projections_only_expose_drafts_and_sent_items.md)
- [message_row_projects_containing_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/message_row_projects_containing_folder_ids.md)
- [draft_message_row_projects_mf_unsent_from_canonical_mailbox_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/draft_message_row_projects_mf_unsent_from_canonical_mailbox_state.md)
- [message_row_client_submit_time_falls_back_to_received_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/message_row_client_submit_time_falls_back_to_received_time.md)
- [normal_message_row_projects_outlook_inbox_view_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_outlook_inbox_view_columns.md)
- [normal_message_row_projects_microsoft_view_descriptor_string8_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_microsoft_view_descriptor_string8_columns.md)