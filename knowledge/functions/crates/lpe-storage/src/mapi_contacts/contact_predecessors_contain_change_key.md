---
type: Rust Function
title: contact_predecessors_contain_change_key
resource: crates/lpe-storage/src/mapi_contacts.rs#L1119-L1131
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_contacts/split_contact_xid
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
---

# Signature

`fn contact_predecessors_contain_change_key( entries: &ContactPredecessors, change_key: &[u8], ) -> Result<bool>`

# Calls

- [split_contact_xid](../../../../../functions/crates/lpe-storage/src/mapi_contacts/split_contact_xid.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)