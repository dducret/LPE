---
type: Rust Method
title: calendar_update_name
resource: crates/lpe-jmap/src/calendar.rs#L301-L325
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/has_jmap_property_patch
  - functions/crates/lpe-jmap/src/calendar/calendar_to_value
  - functions/crates/lpe-jmap/src/calendar/calendar_properties
  - functions/crates/lpe-jmap/src/convert/apply_jmap_property_patch
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_collection_name
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set
---

# Signature

`async fn calendar_update_name( &self, account_id: Uuid, calendar_id: &str, value: Value, ) -> Result<String>`

# Calls

- [has_jmap_property_patch](../../../../../../functions/crates/lpe-jmap/src/convert/has_jmap_property_patch.md)
- [calendar_to_value](../../../../../../functions/crates/lpe-jmap/src/calendar/calendar_to_value.md)
- [calendar_properties](../../../../../../functions/crates/lpe-jmap/src/calendar/calendar_properties.md)
- [apply_jmap_property_patch](../../../../../../functions/crates/lpe-jmap/src/convert/apply_jmap_property_patch.md)
- [parse_calendar_collection_name](../../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_collection_name.md)

# Called by

- [handle_calendar_set](../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set.md)