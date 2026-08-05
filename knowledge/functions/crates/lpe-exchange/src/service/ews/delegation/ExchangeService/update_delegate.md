---
type: Rust Method
title: update_delegate
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L44-L78
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/delegation/validate_delegate_mailbox_owner
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_meeting_delivery
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_users
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn update_delegate( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [validate_delegate_mailbox_owner](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/validate_delegate_mailbox_owner.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [parse_delegate_meeting_delivery](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_meeting_delivery.md)
- [parse_ews_delegate_users](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_users.md)
- [mutate_ews_delegates](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)