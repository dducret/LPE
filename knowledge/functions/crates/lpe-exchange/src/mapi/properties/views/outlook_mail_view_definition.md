---
type: Rust Function
title: outlook_mail_view_definition
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L171-L280
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_handoff_descriptor_summary_reports_outlook_view_shape
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_covers_outlook_mail_view_columns
  - functions/crates/lpe-exchange/src/mapi/properties/tests/messages_view_definition_matches_outlook_visible_inbox_projection
  - functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_compact_view_definition_binary_matches_visible_trace_contract
  - functions/crates/lpe-exchange/src/mapi/properties/tests/view_descriptor_named_string_column_matches_microsoft_example
  - functions/crates/lpe-exchange/src/mapi/properties/tests/common_view_sent_to_descriptor_uses_recipient_columns
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_view_named_view_sync_projects_canonical_descriptor_properties
---

# Signature

`pub(in crate::mapi) fn outlook_mail_view_definition(view_name: &str) -> ViewDefinition`

# Called by

- [view_handoff_descriptor_summary_reports_outlook_view_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_handoff_descriptor_summary_reports_outlook_view_shape.md)
- [normal_message_column_support_covers_outlook_mail_view_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_covers_outlook_mail_view_columns.md)
- [messages_view_definition_matches_outlook_visible_inbox_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/messages_view_definition_matches_outlook_visible_inbox_projection.md)
- [outlook_compact_view_definition_binary_matches_visible_trace_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_compact_view_definition_binary_matches_visible_trace_contract.md)
- [view_descriptor_named_string_column_matches_microsoft_example](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/view_descriptor_named_string_column_matches_microsoft_example.md)
- [common_view_sent_to_descriptor_uses_recipient_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/common_view_sent_to_descriptor_uses_recipient_columns.md)
- [outlook_folder_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [common_view_named_view_sync_projects_canonical_descriptor_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_view_named_view_sync_projects_canonical_descriptor_properties.md)