---
type: Rust Function
title: attendees_from_nodes
resource: crates/lpe-activesync/src/service/application_data.rs#L311-L358
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/service/application_data/parse_event_input
---

# Signature

`fn attendees_from_nodes(application_data: &WbxmlNode) -> Option<CalendarParticipantsMetadata>`

# Calls

- [text_value](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [parse_event_input](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_event_input.md)