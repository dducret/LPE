---
type: Rust Function
title: visible_address_book_email
resource: crates/lpe-exchange/src/service/ews/directory.rs#L290-L302
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_user_photo
---

# Signature

`pub(in crate::service) fn visible_address_book_email( principal: &AccountPrincipal, entries: &[ExchangeAddressBookEntry], email: &str, ) -> bool`

# Called by

- [get_user_photo](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_user_photo.md)