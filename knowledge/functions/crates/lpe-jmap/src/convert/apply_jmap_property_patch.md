---
type: Rust Function
title: apply_jmap_property_patch
resource: crates/lpe-jmap/src/convert.rs#L44-L63
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/apply_jmap_property_path
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input
  - functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input
---

# Signature

`pub(crate) fn apply_jmap_property_patch( target: &mut Value, patch: &Map<String, Value>, ) -> Result<()>`

# Calls

- [apply_jmap_property_path](../../../../../functions/crates/lpe-jmap/src/convert/apply_jmap_property_path.md)
- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [calendar_update_name](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name.md)
- [calendar_event_update_input](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input.md)
- [contact_update_input](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input.md)