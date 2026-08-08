---
type: Rust Function
title: nspi_lookup_matches_principal
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1146-L1156
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/mapi/nspi/principal_legacy_dn_aliases
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
---

# Signature

`pub(in crate::mapi) fn nspi_lookup_matches_principal( value: &str, principal: &AccountPrincipal, ) -> bool`

# Calls

- [normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [principal_legacy_dn_aliases](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/principal_legacy_dn_aliases.md)

# Called by

- [resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_dn_to_mid_match](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match.md)
- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)