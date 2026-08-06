---
type: Rust Function
title: contact_input_from_mapi_with_deletions
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L698-L777
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/contact/remove_labeled_contact_values
  - functions/crates/lpe-exchange/src/mapi/properties/contact/remove_contact_email_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/staged_contact_commit_input
---

# Signature

`pub(in crate::mapi) fn contact_input_from_mapi_with_deletions( account_id: Uuid, id: Option<Uuid>, existing: &AccessibleContact, properties: &HashMap<u32, MapiValue>, deleted_properties: &HashSet<u32>, ) -> Result<UpsertClientContactInput>`

# Calls

- [contact_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [remove_labeled_contact_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/remove_labeled_contact_values.md)
- [remove_contact_email_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/remove_contact_email_index.md)

# Called by

- [staged_contact_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/staged_contact_commit_input.md)