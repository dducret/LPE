---
type: Rust Function
title: principal_legacy_dn_aliases
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1155-L1168
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/push_principal_legacy_dn_alias
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal
---

# Signature

`pub(in crate::mapi) fn principal_legacy_dn_aliases(principal: &AccountPrincipal) -> Vec<String>`

# Calls

- [push_principal_legacy_dn_alias](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/push_principal_legacy_dn_alias.md)

# Called by

- [log_rop_logon_request_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity.md)
- [nspi_lookup_matches_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal.md)