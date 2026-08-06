---
type: Rust Function
title: split_contact_xid
resource: crates/lpe-storage/src/mapi_contacts.rs#L846-L854
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/parse_contact_predecessor_change_list
  - functions/crates/lpe-storage/src/mapi_contacts/contact_predecessors_contain_change_key
---

# Signature

`fn split_contact_xid(bytes: &[u8]) -> Result<([u8; 16], &[u8])>`

# Called by

- [parse_contact_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_contacts/parse_contact_predecessor_change_list.md)
- [contact_predecessors_contain_change_key](../../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_predecessors_contain_change_key.md)