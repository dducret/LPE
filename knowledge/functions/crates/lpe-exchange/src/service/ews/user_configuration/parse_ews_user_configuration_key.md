---
type: Rust Function
title: parse_ews_user_configuration_key
resource: crates/lpe-exchange/src/service/ews/user_configuration.rs#L271-L317
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/get_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/delete_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_upsert
---

# Signature

`pub(in crate::service) fn parse_ews_user_configuration_key( request: &str, ) -> Result<EwsUserConfigurationKey>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [attribute_value_after](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)

# Called by

- [get_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/get_user_configuration.md)
- [delete_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/delete_user_configuration.md)
- [parse_ews_user_configuration_upsert](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_upsert.md)