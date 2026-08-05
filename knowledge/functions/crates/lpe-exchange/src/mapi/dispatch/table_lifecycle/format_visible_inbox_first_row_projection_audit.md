---
type: Rust Function
title: format_visible_inbox_first_row_projection_audit
resource: crates/lpe-exchange/src/mapi/dispatch/table_lifecycle.rs#L58-L147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
---

# Signature

`pub(super) fn format_visible_inbox_first_row_projection_audit( position: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, columns: &[u32], mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> VisibleInboxProjectionAudit`

# Calls

- [emails_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [restriction_matches_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [select_query_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)
- [serialize_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row.md)
- [standard_property_row_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)

# Called by

- [append_set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)