---
type: Rust Function
title: get_user_availability_success_response
resource: crates/lpe-exchange/src/service/ews/availability.rs#L59-L99
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability
---

# Signature

`pub(in crate::service) fn get_user_availability_success_response( events: &[AccessibleEvent], suggestions_response: Option<&str>, ) -> String`

# Called by

- [get_user_availability](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability.md)