---
type: Rust Function
title: message_tracking_query_text
resource: crates/lpe-exchange/src/service/ews/message_tracking.rs#L116-L128
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/find_message_tracking_report
---

# Signature

`pub(in crate::service) fn message_tracking_query_text(request: &str) -> String`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [find_message_tracking_report](../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/find_message_tracking_report.md)