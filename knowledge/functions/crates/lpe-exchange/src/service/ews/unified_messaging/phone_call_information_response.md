---
type: Rust Function
title: phone_call_information_response
resource: crates/lpe-exchange/src/service/ews/unified_messaging.rs#L130-L146
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/get_phone_call_information
---

# Signature

`pub(in crate::service) fn phone_call_information_response( call: &EwsUnifiedMessagingCall, ) -> String`

# Called by

- [get_phone_call_information](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/get_phone_call_information.md)