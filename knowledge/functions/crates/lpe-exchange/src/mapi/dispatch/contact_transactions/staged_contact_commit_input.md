---
type: Rust Function
title: staged_contact_commit_input
resource: crates/lpe-exchange/src/mapi/dispatch/contact_transactions.rs#L90-L141
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi_with_deletions
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact
---

# Signature

`pub(super) fn staged_contact_commit_input( principal: &AccountPrincipal, contact: &crate::mapi_store::MapiContact, transaction: &MapiContactTransaction, force_save: bool, ) -> Result<lpe_storage::MapiContactCommitInput>`

# Calls

- [split_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values.md)
- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [contact_input_from_mapi_with_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi_with_deletions.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [property_type_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)

# Called by

- [save_existing_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact.md)