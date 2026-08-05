---
type: Rust Function
title: table_column_support_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L995-L1017
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_table_column_support_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/associated_contents_table_column_support_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/calendar_event_table_column_support_summary
---

# Signature

`fn table_column_support_summary(columns: &[u32], is_backed: impl Fn(u32) -> bool) -> String`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)

# Called by

- [normal_message_table_column_support_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_table_column_support_summary.md)
- [associated_contents_table_column_support_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/associated_contents_table_column_support_summary.md)
- [calendar_event_table_column_support_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/calendar_event_table_column_support_summary.md)