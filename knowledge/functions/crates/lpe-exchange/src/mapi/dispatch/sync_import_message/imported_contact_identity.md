---
type: Rust Function
title: imported_contact_identity
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_message.rs#L88-L102
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`fn imported_contact_identity( properties: &HashMap<u32, MapiValue>, imported_message_id: u64, ) -> Result<MapiContactImportedIdentity>`

# Calls

- [imported_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity.md)

# Called by

- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)