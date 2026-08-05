---
type: Rust Function
title: phone_call_id_from_request
resource: crates/lpe-exchange/src/service/ews/unified_messaging.rs#L107-L112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/get_phone_call_information
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/disconnect_phone_call
---

# Signature

`pub(in crate::service) fn phone_call_id_from_request(request: &str) -> Option<String>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [get_phone_call_information](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/get_phone_call_information.md)
- [disconnect_phone_call](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/disconnect_phone_call.md)