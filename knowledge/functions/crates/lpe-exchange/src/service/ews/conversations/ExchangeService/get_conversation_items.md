---
type: Rust Method
title: get_conversation_items
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L28-L55
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/conversations/requested_conversation_ids
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/conversation_source_emails
  - functions/crates/lpe-exchange/src/service/ews/conversations/filter_ignored_conversation_folders
  - functions/crates/lpe-exchange/src/service/ews/conversations/get_conversation_items_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_conversation_items( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_conversation_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/requested_conversation_ids.md)
- [conversation_source_emails](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/conversation_source_emails.md)
- [filter_ignored_conversation_folders](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/filter_ignored_conversation_folders.md)
- [get_conversation_items_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/get_conversation_items_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)