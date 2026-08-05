---
type: Rust Function
title: find_people_response
resource: crates/lpe-exchange/src/service/ews/directory.rs#L144-L176
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/directory/find_people_query_text
  - functions/crates/lpe-exchange/src/service/ews/directory/visible_persona_entries
  - functions/crates/lpe-exchange/src/service/ews/directory/address_book_entry_matches
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/find_people
---

# Signature

`pub(in crate::service) fn find_people_response( principal: &AccountPrincipal, request: &str, entries: &[ExchangeAddressBookEntry], ) -> String`

# Calls

- [find_people_query_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/find_people_query_text.md)
- [visible_persona_entries](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/visible_persona_entries.md)
- [address_book_entry_matches](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/address_book_entry_matches.md)

# Called by

- [find_people](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/find_people.md)