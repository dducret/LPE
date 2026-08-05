---
type: Rust Function
title: requested_persona_id
resource: crates/lpe-exchange/src/service/ews/directory.rs#L382-L390
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/get_persona_response
---

# Signature

`fn requested_persona_id(request: &str) -> Option<String>`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [get_persona_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/get_persona_response.md)