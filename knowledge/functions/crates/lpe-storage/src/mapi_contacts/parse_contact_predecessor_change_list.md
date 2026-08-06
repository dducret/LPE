---
type: Rust Function
title: parse_contact_predecessor_change_list
resource: crates/lpe-storage/src/mapi_contacts.rs#L816-L844
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/mapi_contacts/split_contact_xid
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
---

# Signature

`fn parse_contact_predecessor_change_list(bytes: &[u8]) -> Result<ContactPredecessors>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [split_contact_xid](../../../../../functions/crates/lpe-storage/src/mapi_contacts/split_contact_xid.md)

# Called by

- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)