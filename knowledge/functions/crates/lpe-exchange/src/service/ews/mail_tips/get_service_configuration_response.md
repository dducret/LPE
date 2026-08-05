---
type: Rust Function
title: get_service_configuration_response
resource: crates/lpe-exchange/src/service/ews/mail_tips.rs#L113-L159
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/service_configuration_success_message
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/service_configuration_error_message
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_service_configuration
---

# Signature

`pub(in crate::service) fn get_service_configuration_response( configs: &[RequestedServiceConfiguration], ) -> String`

# Calls

- [service_configuration_success_message](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/service_configuration_success_message.md)
- [service_configuration_error_message](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/service_configuration_error_message.md)

# Called by

- [get_service_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_service_configuration.md)