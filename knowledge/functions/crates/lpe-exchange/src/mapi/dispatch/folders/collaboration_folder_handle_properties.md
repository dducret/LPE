---
type: Rust Function
title: collaboration_folder_handle_properties
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L946-L988
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
---

# Signature

`pub(super) fn collaboration_folder_handle_properties( folder: &crate::mapi_store::MapiCollaborationFolder, ) -> HashMap<u32, MapiValue>`

# Calls

- [collaboration_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)