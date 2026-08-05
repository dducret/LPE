---
type: Rust Function
title: resolve_names_response
resource: crates/lpe-exchange/src/service/ews/directory.rs#L95-L142
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/directory/resolve_names_no_results_response
  - functions/crates/lpe-exchange/src/service/ews/directory/address_book_entry_matches
  - functions/crates/lpe-exchange/src/service/ews/directory/address_book_lookup_matches_principal
---

# Signature

`pub(in crate::service) fn resolve_names_response( principal: &AccountPrincipal, request: &str, entries: &[ExchangeAddressBookEntry], ) -> String`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [resolve_names_no_results_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/resolve_names_no_results_response.md)
- [address_book_entry_matches](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/address_book_entry_matches.md)
- [address_book_lookup_matches_principal](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/address_book_lookup_matches_principal.md)