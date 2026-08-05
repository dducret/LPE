---
type: Rust Function
title: requested_service_configurations
resource: crates/lpe-exchange/src/service/ews/mail_tips.rs#L193-L237
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_service_configuration
---

# Signature

`pub(in crate::service) fn requested_service_configurations( request: &str, ) -> Vec<RequestedServiceConfiguration>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [get_service_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_service_configuration.md)