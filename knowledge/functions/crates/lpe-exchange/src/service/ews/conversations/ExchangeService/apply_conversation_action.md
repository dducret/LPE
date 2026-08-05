---
type: Rust Method
title: apply_conversation_action
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L57-L186
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/conversations/parse_conversation_actions
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn apply_conversation_action( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [parse_conversation_actions](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/parse_conversation_actions.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [simple_operation_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)