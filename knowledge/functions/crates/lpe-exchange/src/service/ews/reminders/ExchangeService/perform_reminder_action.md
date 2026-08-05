---
type: Rust Method
title: perform_reminder_action
resource: crates/lpe-exchange/src/service/ews/reminders.rs#L23-L146
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/reminders/parse_reminder_item_id
  - functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn perform_reminder_action( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [requested_item_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [parse_reminder_item_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/reminders/parse_reminder_item_id.md)
- [simple_operation_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)