---
type: Rust Function
title: logon_special_folder_contract_issues
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L882-L892
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_entries
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/logon_special_folder_contract_reports_mismatched_inbox
---

# Signature

`pub(super) fn logon_special_folder_contract_issues(folder_ids: &[u64]) -> String`

# Calls

- [logon_special_folder_contract_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_entries.md)

# Called by

- [summarize_logon_response_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)
- [logon_special_folder_contract_reports_mismatched_inbox](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/logon_special_folder_contract_reports_mismatched_inbox.md)