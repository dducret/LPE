---
type: Rust Function
title: requested_message_tracking_report_id
resource: crates/lpe-exchange/src/service/ews/message_tracking.rs#L130-L141
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/get_message_tracking_report
---

# Signature

`pub(in crate::service) fn requested_message_tracking_report_id(request: &str) -> Option<String>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)

# Called by

- [get_message_tracking_report](../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/ExchangeService/get_message_tracking_report.md)