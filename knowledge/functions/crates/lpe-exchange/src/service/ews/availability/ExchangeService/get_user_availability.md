---
type: Rust Method
title: get_user_availability
resource: crates/lpe-exchange/src/service/ews/availability.rs#L8-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/responses/get_user_availability_error_response
  - functions/crates/lpe-exchange/src/service/ews/availability/requested_availability_window
  - functions/crates/lpe-exchange/src/service/ews/availability/event_overlaps_window
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime
  - functions/crates/lpe-exchange/src/service/ews/availability/get_user_availability_success_response
  - functions/crates/lpe-exchange/src/service/ews/availability/availability_suggestions_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_user_availability( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_content](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [get_user_availability_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/get_user_availability_error_response.md)
- [requested_availability_window](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/requested_availability_window.md)
- [event_overlaps_window](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/event_overlaps_window.md)
- [ews_datetime](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime.md)
- [get_user_availability_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/get_user_availability_success_response.md)
- [availability_suggestions_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/availability_suggestions_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)