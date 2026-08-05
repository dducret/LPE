---
type: Rust Function
title: service_configuration_success_message
resource: crates/lpe-exchange/src/service/ews/mail_tips.rs#L239-L251
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/get_service_configuration_response
---

# Signature

`fn service_configuration_success_message(configuration_name: &str, payload: &str) -> String`

# Called by

- [get_service_configuration_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/get_service_configuration_response.md)