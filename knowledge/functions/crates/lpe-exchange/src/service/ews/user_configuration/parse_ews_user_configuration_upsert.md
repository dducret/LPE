---
type: Rust Function
title: parse_ews_user_configuration_upsert
resource: crates/lpe-exchange/src/service/ews/user_configuration.rs#L319-L341
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_dictionary
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/create_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/update_user_configuration
---

# Signature

`pub(in crate::service) fn parse_ews_user_configuration_upsert( principal: &AccountPrincipal, request: &str, ) -> Result<UpsertEwsUserConfigurationInput>`

# Calls

- [parse_ews_user_configuration_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key.md)
- [parse_ews_user_configuration_dictionary](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_dictionary.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [create_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/create_user_configuration.md)
- [update_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/update_user_configuration.md)