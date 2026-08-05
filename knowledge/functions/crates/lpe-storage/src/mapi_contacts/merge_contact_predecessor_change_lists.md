---
type: Rust Function
title: merge_contact_predecessor_change_lists
resource: crates/lpe-storage/src/mapi_contacts.rs#L821-L845
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
---

# Signature

`fn merge_contact_predecessor_change_lists( mut current: ContactPredecessors, imported: ContactPredecessors, ) -> Result<Vec<u8>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)