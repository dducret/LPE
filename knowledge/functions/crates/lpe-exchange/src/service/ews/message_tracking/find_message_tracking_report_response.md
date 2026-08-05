---
type: Rust Function
title: find_message_tracking_report_response
resource: crates/lpe-exchange/src/service/ews/message_tracking.rs#L49-L69
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/find_message_tracking_report
---

# Signature

`pub(in crate::service) fn find_message_tracking_report_response( reports: &[EwsMessageTrackingReport], ) -> String`

# Called by

- [find_message_tracking_report](../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/find_message_tracking_report.md)