---
type: Rust Function
title: note_named_property_value
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L114-L122
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_color_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_property_value
---

# Signature

`pub(in crate::mapi) fn note_named_property_value( note: &ClientNote, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [note_color_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_color_value.md)

# Called by

- [note_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_property_value.md)