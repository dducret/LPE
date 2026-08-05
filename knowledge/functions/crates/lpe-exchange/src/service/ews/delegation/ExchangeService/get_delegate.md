---
type: Rust Method
title: get_delegate
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L80-L108
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/delegation/validate_delegate_mailbox_owner
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_user_id_emails
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_delegates
  - functions/crates/lpe-exchange/src/service/ews/delegation/get_delegate_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_delegate( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [validate_delegate_mailbox_owner](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/validate_delegate_mailbox_owner.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [parse_delegate_user_id_emails](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_user_id_emails.md)
- [fetch_ews_delegates](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_delegates.md)
- [get_delegate_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/get_delegate_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)