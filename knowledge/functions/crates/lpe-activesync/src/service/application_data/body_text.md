---
type: Rust Function
title: body_text
resource: crates/lpe-activesync/src/service/application_data.rs#L247-L256
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/service/application_data/parse_contact_input
  - functions/crates/lpe-activesync/src/service/application_data/parse_event_input
---

# Signature

`fn body_text(application_data: &WbxmlNode) -> Option<String>`

# Calls

- [text_value](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [parse_contact_input](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_contact_input.md)
- [parse_event_input](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_event_input.md)