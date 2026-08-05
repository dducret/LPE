---
type: Rust Function
title: logon_store_state
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1140-L1147
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity
---

# Signature

`fn logon_store_state(request: &RopLogonRequest) -> u32`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [log_rop_logon_request_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity.md)