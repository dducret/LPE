---
type: Rust Function
title: availability_suggestions_response
resource: crates/lpe-exchange/src/service/ews/availability.rs#L101-L129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/availability/requested_availability_window
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability
---

# Signature

`pub(in crate::service) fn availability_suggestions_response(request: &str) -> Option<String>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [requested_availability_window](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/requested_availability_window.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [get_user_availability](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability.md)