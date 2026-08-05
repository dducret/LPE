---
type: Rust Function
title: log_rop_logon_request_identity
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1085-L1129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/decode_logon_identity_bytes
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/mapi/nspi/principal_legacy_dn_aliases
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_request_shape
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_open_flags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_store_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/projected_logon_response_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response
---

# Signature

`pub(super) fn log_rop_logon_request_identity( principal: &AccountPrincipal, request_id: &str, request: &RopLogonRequest, )`

# Calls

- [decode_logon_identity_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/decode_logon_identity_bytes.md)
- [normalize_nspi_lookup_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [principal_legacy_dn_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/principal_legacy_dn_aliases.md)
- [format_logon_request_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_request_shape.md)
- [logon_open_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_open_flags.md)
- [logon_store_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_store_state.md)
- [projected_logon_response_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/projected_logon_response_flags.md)

# Called by

- [append_logon_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response.md)