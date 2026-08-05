---
type: Rust Method
title: get_user_photo
resource: crates/lpe-exchange/src/service/ews/directory.rs#L44-L70
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/responses/get_user_photo_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/service/ews/directory/visible_address_book_email
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_user_photo( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [get_user_photo_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/get_user_photo_error_response.md)
- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [visible_address_book_email](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/visible_address_book_email.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)