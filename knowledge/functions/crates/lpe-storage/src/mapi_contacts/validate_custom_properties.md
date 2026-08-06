---
type: Rust Function
title: validate_custom_properties
resource: crates/lpe-storage/src/mapi_contacts.rs#L523-L534
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
---

# Signature

`fn validate_custom_properties(values: &[MapiContactCustomPropertyValue]) -> Result<()>`

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)