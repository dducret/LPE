---
type: Rust Function
title: rop_restriction_from_json_clause
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L862-L894
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_content_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/property_tag_for_search_field
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_not_content_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/string_search_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_to_rop
---

# Signature

`fn rop_restriction_from_json_clause(clause: &Value, use_unicode: bool) -> Result<Vec<u8>, u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [rop_content_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_content_restriction.md)
- [property_tag_for_search_field](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/property_tag_for_search_field.md)
- [rop_property_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction.md)
- [rop_not_content_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_not_content_restriction.md)
- [string_search_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/string_search_property_tag.md)

# Called by

- [bounded_search_criteria_to_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_to_rop.md)