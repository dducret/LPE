---
type: Rust Method
title: get_service_configuration
resource: crates/lpe-exchange/src/service/ews/mail_tips.rs#L68-L74
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_service_configurations
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/get_service_configuration_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_service_configuration( &self, request: &str, ) -> Result<String>`

# Calls

- [requested_service_configurations](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_service_configurations.md)
- [get_service_configuration_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/get_service_configuration_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)