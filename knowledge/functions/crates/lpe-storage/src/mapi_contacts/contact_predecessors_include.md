---
type: Rust Function
title: contact_predecessors_include
resource: crates/lpe-storage/src/mapi_contacts.rs#L1101-L1117
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
---

# Signature

`fn contact_predecessors_include( candidate: &ContactPredecessors, predecessor: &ContactPredecessors, ) -> Result<bool>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)