---
type: Rust Method
title: fetch_ews_message_tracking_reports
resource: crates/lpe-exchange/src/tests/mod.rs#L5353-L5397
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/find_message_tracking_report
---

# Signature

`fn fetch_ews_message_tracking_reports<'a>( &'a self, principal: &'a AccountPrincipal, query_text: &'a str, limit: usize, ) -> StoreFuture<'a, Vec<EwsMessageTrackingReport>>`

# Called by

- [find_message_tracking_report](../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/find_message_tracking_report.md)