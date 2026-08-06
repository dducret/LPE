---
type: Rust Function
title: remove_contact_address
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L648-L655
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_addresses_json
---

# Signature

`fn remove_contact_address(rows: &mut Vec<serde_json::Value>, label: &str)`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [ews_updated_contact_addresses_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_addresses_json.md)