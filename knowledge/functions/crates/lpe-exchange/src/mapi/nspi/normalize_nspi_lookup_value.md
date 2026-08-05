---
type: Rust Function
title: normalize_nspi_lookup_value
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1497-L1499
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/normalization/normalize_smtp_lookup_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_dn_to_mid_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_ascii_lookup_values
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values
  - functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address
---

# Signature

`pub(in crate::mapi) fn normalize_nspi_lookup_value(value: &str) -> String`

# Calls

- [normalize_smtp_lookup_value](../../../../../../functions/crates/lpe-domain/src/normalization/normalize_smtp_lookup_value.md)

# Called by

- [log_rop_logon_request_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity.md)
- [parse_resolve_names_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values.md)
- [nspi_match_dn_to_mid_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_dn_to_mid_entry.md)
- [nspi_lookup_matches_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal.md)
- [nspi_entry_match_rank](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank.md)
- [scan_ascii_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_ascii_lookup_values.md)
- [scan_utf16_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values.md)
- [parse_dn_to_mid_names](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names.md)
- [legacy_dn_recipient_address](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address.md)