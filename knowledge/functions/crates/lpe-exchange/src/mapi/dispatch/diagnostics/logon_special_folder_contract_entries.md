---
type: Rust Function
title: logon_special_folder_contract_entries
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L894-L925
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_special_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_issues
---

# Signature

`fn logon_special_folder_contract_entries( folder_ids: &[u64], ) -> Vec<(usize, &'static str, u64, u64)>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [format_logon_special_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_special_folder_contract.md)
- [logon_special_folder_contract_issues](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_issues.md)