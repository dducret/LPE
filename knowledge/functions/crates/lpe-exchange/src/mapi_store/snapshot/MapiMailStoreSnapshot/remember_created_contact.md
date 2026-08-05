---
type: Rust Method
title: remember_created_contact
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L532-L569
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact
---

# Signature

`pub(crate) fn remember_created_contact( &mut self, folder_id: u64, contact: AccessibleContact, identity: MapiIdentityRecord, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [save_pending_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact.md)