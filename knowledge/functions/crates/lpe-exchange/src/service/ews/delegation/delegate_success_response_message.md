---
type: Rust Function
title: delegate_success_response_message
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L234-L250
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ews_delegate_user_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates
  - functions/crates/lpe-exchange/src/service/ews/delegation/get_delegate_response
---

# Signature

`pub(in crate::service) fn delegate_success_response_message( delegate: &EwsDelegate, include_delegate: bool, ) -> String`

# Calls

- [ews_delegate_user_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ews_delegate_user_xml.md)

# Called by

- [mutate_ews_delegates](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates.md)
- [get_delegate_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/get_delegate_response.md)