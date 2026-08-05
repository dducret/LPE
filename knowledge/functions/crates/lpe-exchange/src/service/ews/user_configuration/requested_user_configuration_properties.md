---
type: Rust Function
title: requested_user_configuration_properties
resource: crates/lpe-exchange/src/service/ews/user_configuration.rs#L220-L243
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/get_user_configuration_response
---

# Signature

`fn requested_user_configuration_properties(request: &str) -> RequestedUserConfigurationProperties`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)

# Called by

- [get_user_configuration_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/get_user_configuration_response.md)