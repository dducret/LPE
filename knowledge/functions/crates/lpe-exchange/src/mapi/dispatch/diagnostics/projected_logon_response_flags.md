---
type: Rust Function
title: projected_logon_response_flags
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1149-L1155
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/logon/private_logon_response_logon_flags
  - functions/crates/lpe-exchange/src/mapi/rop/logon/public_folder_logon_response_logon_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_request_shape
---

# Signature

`fn projected_logon_response_flags(request_logon_flags: u8) -> u8`

# Calls

- [private_logon_response_logon_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/private_logon_response_logon_flags.md)
- [public_folder_logon_response_logon_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/public_folder_logon_response_logon_flags.md)

# Called by

- [log_rop_logon_request_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity.md)
- [format_logon_request_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_request_shape.md)