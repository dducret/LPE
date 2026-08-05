---
type: Rust Function
title: normal_message_defaulted_column_detail
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L219-L251
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_table_column_is_backed
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_backs_outlook_auxiliary_flags
---

# Signature

`pub(super) fn normal_message_defaulted_column_detail(columns: &[u32]) -> String`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [normal_message_table_column_is_backed](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_table_column_is_backed.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)

# Called by

- [normal_message_column_support_backs_outlook_auxiliary_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_backs_outlook_auxiliary_flags.md)