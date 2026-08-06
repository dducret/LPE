---
type: Rust Function
title: apply_contact_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/contact_transactions.rs#L143-L171
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values
---

# Signature

`fn apply_contact_property_values( pending: &mut HashMap<u32, MapiValue>, deleted: &mut HashSet<u32>, values: &[(u32, MapiValue)], )`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [stage_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values.md)