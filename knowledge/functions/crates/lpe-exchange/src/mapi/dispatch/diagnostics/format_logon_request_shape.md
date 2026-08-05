---
type: Rust Function
title: format_logon_request_shape
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1157-L1170
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/projected_logon_response_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity
---

# Signature

`fn format_logon_request_shape(request: &RopLogonRequest) -> String`

# Calls

- [projected_logon_response_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/projected_logon_response_flags.md)

# Called by

- [log_rop_logon_request_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity.md)