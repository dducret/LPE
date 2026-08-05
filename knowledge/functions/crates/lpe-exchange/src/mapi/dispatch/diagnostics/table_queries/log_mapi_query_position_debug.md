---
type: Rust Function
title: log_mapi_query_position_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L1031-L1208
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_option
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_mapi_query_position_debug( principal: &AccountPrincipal, session: &MapiSession, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, response: &[u8], mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [effective_contents_table_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [outlook_view_descriptor_visible_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags.md)
- [format_calendar_event_query_position_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [format_debug_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags.md)
- [format_debug_restriction_option](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_option.md)
- [format_debug_restriction_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_property_tags.md)
- [format_normal_message_query_row_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_inbox_view_descriptor_behavior_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [format_default_view_table_compatibility_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract.md)

# Called by

- [append_table_control_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)