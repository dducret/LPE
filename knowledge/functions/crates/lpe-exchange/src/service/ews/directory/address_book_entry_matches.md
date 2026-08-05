---
type: Rust Function
title: address_book_entry_matches
resource: crates/lpe-exchange/src/service/ews/directory.rs#L469-L486
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/directory/normalize_address_book_lookup
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/resolve_names_response
  - functions/crates/lpe-exchange/src/service/ews/directory/find_people_response
  - functions/crates/lpe-exchange/src/service/ews/directory/expand_dl_response
---

# Signature

`fn address_book_entry_matches( entry: &ExchangeAddressBookEntry, value: &str, allow_partial: bool, ) -> bool`

# Calls

- [normalize_address_book_lookup](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/normalize_address_book_lookup.md)

# Called by

- [resolve_names_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/resolve_names_response.md)
- [find_people_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/find_people_response.md)
- [expand_dl_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/expand_dl_response.md)