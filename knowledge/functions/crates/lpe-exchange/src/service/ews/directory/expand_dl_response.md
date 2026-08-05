---
type: Rust Function
title: expand_dl_response
resource: crates/lpe-exchange/src/service/ews/directory.rs#L223-L288
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_first_mailbox
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/directory/address_book_entry_matches
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/expand_dl
---

# Signature

`pub(in crate::service) fn expand_dl_response( principal: &AccountPrincipal, request: &str, entries: &[ExchangeAddressBookEntry], ) -> String`

# Calls

- [parse_first_mailbox](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_first_mailbox.md)
- [operation_error_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [address_book_entry_matches](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/address_book_entry_matches.md)

# Called by

- [expand_dl](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/expand_dl.md)