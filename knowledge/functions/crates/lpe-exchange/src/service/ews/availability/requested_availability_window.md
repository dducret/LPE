---
type: Rust Function
title: requested_availability_window
resource: crates/lpe-exchange/src/service/ews/availability.rs#L131-L139
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability
  - functions/crates/lpe-exchange/src/service/ews/availability/availability_suggestions_response
---

# Signature

`pub(in crate::service) fn requested_availability_window( request: &str, ) -> (Option<String>, Option<String>)`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [get_user_availability](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability.md)
- [availability_suggestions_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/availability_suggestions_response.md)