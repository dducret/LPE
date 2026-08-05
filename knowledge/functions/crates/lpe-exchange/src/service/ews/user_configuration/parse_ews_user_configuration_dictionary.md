---
type: Rust Function
title: parse_ews_user_configuration_dictionary
resource: crates/lpe-exchange/src/service/ews/user_configuration.rs#L343-L359
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_upsert
---

# Signature

`fn parse_ews_user_configuration_dictionary(request: &str) -> Result<serde_json::Value>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [parse_ews_user_configuration_upsert](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_upsert.md)