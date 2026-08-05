---
type: Rust Method
title: find_message_tracking_report
resource: crates/lpe-exchange/src/service/ews/message_tracking.rs#L8-L19
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/message_tracking_query_text
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_message_tracking_reports
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/find_message_tracking_report_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn find_message_tracking_report( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [message_tracking_query_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/message_tracking_query_text.md)
- [fetch_ews_message_tracking_reports](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_message_tracking_reports.md)
- [find_message_tracking_report_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/find_message_tracking_report_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)