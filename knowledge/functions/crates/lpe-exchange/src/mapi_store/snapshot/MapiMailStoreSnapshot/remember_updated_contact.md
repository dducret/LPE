---
type: Rust Method
title: remember_updated_contact
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L595-L610
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_contact
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact
---

# Signature

`pub(crate) fn remember_updated_contact( &mut self, folder_id: u64, contact_id: u64, contact: AccessibleContact, identity: MapiIdentityRecord, canonical_modseq: i64, )`

# Calls

- [remember_created_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_contact.md)
- [contact_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id.md)

# Called by

- [save_existing_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact.md)