---
type: Rust Function
title: contact_address_by_label
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L418-L428
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_physical_addresses_xml
---

# Signature

`fn contact_address_by_label<'a>( contact: &'a AccessibleContact, label: &str, ) -> Option<&'a serde_json::Value>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [ews_contact_physical_addresses_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_physical_addresses_xml.md)