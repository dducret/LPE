---
type: Rust Function
title: decode_logon_identity_bytes
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1207-L1213
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity
---

# Signature

`fn decode_logon_identity_bytes(bytes: &[u8]) -> String`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [log_rop_logon_request_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity.md)