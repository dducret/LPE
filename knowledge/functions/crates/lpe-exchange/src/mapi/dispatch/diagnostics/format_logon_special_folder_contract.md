---
type: Rust Function
title: format_logon_special_folder_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L869-L880
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_entries
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop
---

# Signature

`fn format_logon_special_folder_contract(folder_ids: &[u64]) -> String`

# Calls

- [logon_special_folder_contract_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_entries.md)

# Called by

- [summarize_logon_response_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)