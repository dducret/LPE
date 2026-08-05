---
type: Rust Function
title: format_inbox_associated_config_summary
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config.rs#L159-L194
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_config_summary_reports_modeled_startup_rows
---

# Signature

`pub(in crate::mapi::dispatch) fn format_inbox_associated_config_summary( folder_id: u64, associated: bool, snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [debug_associated_table_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [source_key_for_store_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [inbox_associated_config_summary_reports_modeled_startup_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_config_summary_reports_modeled_startup_rows.md)