---
type: Rust Function
title: ews_updated_contact_phones_json
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L673-L683
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phones_json
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
---

# Signature

`fn ews_updated_contact_phones_json( request: &str, contact: &str, existing: &AccessibleContact, ) -> serde_json::Value`

# Calls

- [ews_contact_phones_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_phones_json.md)

# Called by

- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)