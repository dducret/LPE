---
type: Rust Function
title: rop_response_return_value
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L293-L299
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
---

# Signature

`fn rop_response_return_value(response: &[u8]) -> u32`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [log_outlook_contents_table_find_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)