---
type: Rust Function
title: address_book_lookup_matches_principal
resource: crates/lpe-exchange/src/service/ews/directory.rs#L462-L467
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/directory/normalize_address_book_lookup
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/resolve_names_response
---

# Signature

`fn address_book_lookup_matches_principal(value: &str, principal: &AccountPrincipal) -> bool`

# Calls

- [normalize_address_book_lookup](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/normalize_address_book_lookup.md)

# Called by

- [resolve_names_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/resolve_names_response.md)