---
type: Rust Method
title: get_mail_tips
resource: crates/lpe-exchange/src/service/ews/mail_tips.rs#L8-L66
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips_recipients
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/service/ews/oof/oof_projection_from_script
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/get_mail_tips_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_mail_tips( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_mail_tips_recipients](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips_recipients.md)
- [requested_mail_tips](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips.md)
- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [oof_projection_from_script](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/oof_projection_from_script.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get_mail_tips_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/get_mail_tips_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)