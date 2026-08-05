---
type: Rust Function
title: contact_address_value
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L74-L88
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/address_book_details_from_contact
---

# Signature

`fn contact_address_value(contact: &AccessibleContact, keys: &[&str]) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [address_book_details_from_contact](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/address_book_details_from_contact.md)