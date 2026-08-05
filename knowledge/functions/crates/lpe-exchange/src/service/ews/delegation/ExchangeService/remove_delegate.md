---
type: Rust Method
title: remove_delegate
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L110-L173
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/delegation/validate_delegate_mailbox_owner
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_user_id_emails
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/service/ews/delegation/delegate_error_response_message
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/remove_ews_delegate
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  - functions/crates/lpe-exchange/src/service/ews/delegation/delegate_operation_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn remove_delegate( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [validate_delegate_mailbox_owner](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/validate_delegate_mailbox_owner.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [parse_delegate_user_id_emails](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_user_id_emails.md)
- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [delegate_error_response_message](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/delegate_error_response_message.md)
- [remove_ews_delegate](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/remove_ews_delegate.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)
- [delegate_operation_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/delegate_operation_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)