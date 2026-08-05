---
type: Rust Function
title: private_logon_response_logon_flags
resource: crates/lpe-exchange/src/mapi/rop/logon.rs#L10-L12
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/projected_logon_response_flags
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body
---

# Signature

`pub(in crate::mapi) fn private_logon_response_logon_flags(request_logon_flags: u8) -> u8`

# Called by

- [projected_logon_response_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/projected_logon_response_flags.md)
- [rop_logon_response_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body.md)