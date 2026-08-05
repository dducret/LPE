---
type: Rust Function
title: default_associated_config_columns
resource: crates/lpe-exchange/src/mapi/tables/columns.rs#L54-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contents_columns
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_view_handoff_table_contract_reports_no_unpersisted_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/sent_view_handoff_table_contract_reports_no_unpersisted_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/junk_view_handoff_table_contract_reports_no_unpersisted_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/quick_step_view_handoff_table_contract_reports_unsupported_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/contacts_view_handoff_table_contract_reports_no_unpersisted_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_view_handoff_table_contract_reports_client_normal_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/task_note_journal_handoff_contracts_report_standard_visible_columns
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response
  - functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/default_associated_config_columns_cover_required_configuration_contract
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract
---

# Signature

`pub(in crate::mapi) fn default_associated_config_columns() -> Vec<u32>`

# Calls

- [default_contents_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contents_columns.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [effective_contents_table_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [inbox_view_handoff_table_contract_reports_no_unpersisted_default_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_view_handoff_table_contract_reports_no_unpersisted_default_view.md)
- [sent_view_handoff_table_contract_reports_no_unpersisted_default_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/sent_view_handoff_table_contract_reports_no_unpersisted_default_view.md)
- [junk_view_handoff_table_contract_reports_no_unpersisted_default_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/junk_view_handoff_table_contract_reports_no_unpersisted_default_view.md)
- [quick_step_view_handoff_table_contract_reports_unsupported_default_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/quick_step_view_handoff_table_contract_reports_unsupported_default_view.md)
- [contacts_view_handoff_table_contract_reports_no_unpersisted_default_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/contacts_view_handoff_table_contract_reports_no_unpersisted_default_view.md)
- [calendar_view_handoff_table_contract_reports_client_normal_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_view_handoff_table_contract_reports_client_normal_view.md)
- [task_note_journal_handoff_contracts_report_standard_visible_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/task_note_journal_handoff_contracts_report_standard_visible_columns.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response.md)
- [query_rows_response_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [default_associated_config_columns_cover_required_configuration_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/default_associated_config_columns_cover_required_configuration_contract.md)
- [inbox_associated_query_rows_default_columns_cover_required_configuration_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract.md)