---
type: Rust Method
title: mutate_ews_delegates
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L175-L208
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_ews_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/delegate_success_response_message
  - functions/crates/lpe-exchange/src/service/ews/delegation/delegate_error_response_message
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  - functions/crates/lpe-exchange/src/service/ews/delegation/delegate_operation_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/add_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/update_delegate
---

# Signature

`async fn mutate_ews_delegates( &self, operation: &str, actor_email: &str, users: Vec<UpsertEwsDelegateInput>, include_delegate: bool, ) -> Result<String>`

# Calls

- [upsert_ews_delegate](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_ews_delegate.md)
- [delegate_success_response_message](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/delegate_success_response_message.md)
- [delegate_error_response_message](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/delegate_error_response_message.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)
- [delegate_operation_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/delegate_operation_response.md)

# Called by

- [add_delegate](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/add_delegate.md)
- [update_delegate](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/update_delegate.md)