---
type: Rust Method
title: get_message_tracking_report
resource: crates/lpe-exchange/src/service/ews/message_tracking.rs#L21-L46
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/requested_message_tracking_report_id
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_message_tracking_report_detail
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/get_message_tracking_report_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_message_tracking_report( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_message_tracking_report_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/requested_message_tracking_report_id.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [fetch_ews_message_tracking_report_detail](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_message_tracking_report_detail.md)
- [get_message_tracking_report_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/get_message_tracking_report_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)