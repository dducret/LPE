---
type: Rust Method
title: find_conversation
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L8-L26
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/conversation_source_emails
  - functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn find_conversation( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [conversation_source_emails](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/conversation_source_emails.md)
- [find_conversation_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)