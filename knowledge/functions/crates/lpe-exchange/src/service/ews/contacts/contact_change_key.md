---
type: Rust Function
title: contact_change_key
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L3-L5
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
---

# Signature

`pub(in crate::service) fn contact_change_key(contact: &AccessibleContact, version: &str) -> String`

# Calls

- [versioned_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key.md)

# Called by

- [contact_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)