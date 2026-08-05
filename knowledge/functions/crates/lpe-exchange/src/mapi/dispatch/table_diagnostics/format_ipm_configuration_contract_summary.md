---
type: Rust Function
title: format_ipm_configuration_contract_summary
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L60-L115
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/outlook_configuration_prefix_debug_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/ipm_configuration_row_issues
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/ipm_configuration_contract_summary_reports_required_columns_and_streams
---

# Signature

`pub(super) fn format_ipm_configuration_contract_summary( folder_id: u64, associated: bool, columns: &[u32], sort_orders: &[MapiSortOrder], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [outlook_configuration_prefix_debug_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/outlook_configuration_prefix_debug_restriction.md)
- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [is_outlook_configuration_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class.md)
- [missing_debug_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags.md)
- [ipm_configuration_row_issues](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/ipm_configuration_row_issues.md)

# Called by

- [ipm_configuration_contract_summary_reports_required_columns_and_streams](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/ipm_configuration_contract_summary_reports_required_columns_and_streams.md)