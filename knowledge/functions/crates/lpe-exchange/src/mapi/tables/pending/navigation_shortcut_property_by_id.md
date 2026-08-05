---
type: Rust Function
title: navigation_shortcut_property_by_id
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L142-L152
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/filters/property_tag_id_matches
---

# Signature

`fn navigation_shortcut_property_by_id<'a>( properties: &'a HashMap<u32, MapiValue>, property_tag: &u32, ) -> Option<&'a MapiValue>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [property_tag_id_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/property_tag_id_matches.md)