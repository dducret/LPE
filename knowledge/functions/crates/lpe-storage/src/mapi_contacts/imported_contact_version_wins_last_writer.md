---
type: Rust Function
title: imported_contact_version_wins_last_writer
resource: crates/lpe-storage/src/mapi_contacts.rs#L725-L745
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
---

# Signature

`fn imported_contact_version_wins_last_writer( incoming_last_modification_time: u64, incoming_change_key: &[u8], current_last_modification_time: u64, current_change_key: &[u8], ) -> Result<bool>`

# Called by

- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)