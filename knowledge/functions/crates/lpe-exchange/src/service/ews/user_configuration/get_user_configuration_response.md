---
type: Rust Function
title: get_user_configuration_response
resource: crates/lpe-exchange/src/service/ews/user_configuration.rs#L154-L211
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/requested_user_configuration_properties
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ews_user_configuration_dictionary_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/get_user_configuration
---

# Signature

`pub(in crate::service) fn get_user_configuration_response( configuration: &EwsUserConfiguration, request: &str, ) -> String`

# Calls

- [requested_user_configuration_properties](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/requested_user_configuration_properties.md)
- [ews_user_configuration_dictionary_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ews_user_configuration_dictionary_xml.md)

# Called by

- [get_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/get_user_configuration.md)