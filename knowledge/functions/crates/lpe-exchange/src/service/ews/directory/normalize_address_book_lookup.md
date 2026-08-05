---
type: Rust Function
title: normalize_address_book_lookup
resource: crates/lpe-exchange/src/service/ews/directory.rs#L488-L490
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/normalization/normalize_smtp_lookup_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/address_book_lookup_matches_principal
  - functions/crates/lpe-exchange/src/service/ews/directory/address_book_entry_matches
---

# Signature

`fn normalize_address_book_lookup(value: &str) -> String`

# Calls

- [normalize_smtp_lookup_value](../../../../../../../functions/crates/lpe-domain/src/normalization/normalize_smtp_lookup_value.md)

# Called by

- [address_book_lookup_matches_principal](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/address_book_lookup_matches_principal.md)
- [address_book_entry_matches](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/address_book_entry_matches.md)