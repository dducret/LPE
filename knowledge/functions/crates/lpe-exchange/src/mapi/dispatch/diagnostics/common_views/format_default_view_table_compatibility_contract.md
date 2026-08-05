---
type: Rust Function
title: format_default_view_table_compatibility_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L492-L564
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_unsupported_columns_from_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_compatibility_reports_missing_unpersisted_inbox_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_compatibility_does_not_compare_unpersisted_inbox_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_compact_table_compatibility_requires_persisted_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_table_compatibility_does_not_claim_synthetic_descriptor
---

# Signature

`pub(in crate::mapi::dispatch) fn format_default_view_table_compatibility_contract( folder_id: u64, associated: bool, columns: &[u32], sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [debug_advertised_default_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)
- [outlook_folder_view_definition](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_runtime_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [default_view_table_unsupported_columns_from_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_unsupported_columns_from_summary.md)
- [missing_debug_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags.md)

# Called by

- [default_view_table_compatibility_reports_missing_unpersisted_inbox_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_compatibility_reports_missing_unpersisted_inbox_view.md)
- [default_view_table_compatibility_does_not_compare_unpersisted_inbox_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_compatibility_does_not_compare_unpersisted_inbox_view.md)
- [log_mapi_query_position_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)
- [inbox_compact_table_compatibility_requires_persisted_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_compact_table_compatibility_requires_persisted_view.md)
- [calendar_table_compatibility_does_not_claim_synthetic_descriptor](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_table_compatibility_does_not_claim_synthetic_descriptor.md)