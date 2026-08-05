---
type: Rust Method
title: fetch_ews_message_tracking_report_detail
resource: crates/lpe-exchange/src/tests/mod.rs#L5325-L5357
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/get_message_tracking_report
---

# Signature

`fn fetch_ews_message_tracking_report_detail<'a>( &'a self, principal: &'a AccountPrincipal, report_id: &'a str, ) -> StoreFuture<'a, Option<EwsMessageTrackingReportDetail>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [get_message_tracking_report](../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/get_message_tracking_report.md)