---
type: Rust Function
title: normalize_table_property_tag_for_session
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L958-L982
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/is_sharing_local_folder_named_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tags_for_session
---

# Signature

`fn normalize_table_property_tag_for_session(session: &MapiSession, property_tag: u32) -> u32`

# Calls

- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [property_type_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [well_known_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id.md)
- [is_sharing_local_folder_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/is_sharing_local_folder_named_property.md)

# Called by

- [normalize_table_property_tags_for_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tags_for_session.md)