---
type: Rust Function
title: parse_update_message_flags
resource: crates/lpe-exchange/src/service/ews/mail.rs#L293-L311
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/parse_xml_bool
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
---

# Signature

`pub(in crate::service) fn parse_update_message_flags( request: &str, ) -> Result<Option<(Option<bool>, Option<bool>)>>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [parse_xml_bool](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/parse_xml_bool.md)
- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)