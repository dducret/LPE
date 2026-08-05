---
type: Rust Method
title: contact_update_input
resource: crates/lpe-jmap/src/contacts.rs#L473-L497
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/has_jmap_property_patch
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-jmap/src/contacts/contact_to_value
  - functions/crates/lpe-jmap/src/contacts/contact_properties
  - functions/crates/lpe-jmap/src/convert/apply_jmap_property_patch
  called_by:
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set
---

# Signature

`async fn contact_update_input( &self, account_id: Uuid, contact_id: Uuid, value: Value, ) -> Result<UpsertClientContactInput>`

# Calls

- [has_jmap_property_patch](../../../../../../functions/crates/lpe-jmap/src/convert/has_jmap_property_patch.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [contact_to_value](../../../../../../functions/crates/lpe-jmap/src/contacts/contact_to_value.md)
- [contact_properties](../../../../../../functions/crates/lpe-jmap/src/contacts/contact_properties.md)
- [apply_jmap_property_patch](../../../../../../functions/crates/lpe-jmap/src/convert/apply_jmap_property_patch.md)

# Called by

- [handle_contact_set](../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set.md)