---
type: Rust Function
title: imported_hierarchy_existing_mailbox
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L295-L318
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`pub(in crate::mapi) fn imported_hierarchy_existing_mailbox<'a>( hierarchy_values: &[(u32, MapiValue)], display_name: &str, mailboxes: &'a [JmapMailbox], ) -> Option<&'a JmapMailbox>`

# Calls

- [source_key_for_mailbox_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder.md)
- [source_key_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)

# Called by

- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)