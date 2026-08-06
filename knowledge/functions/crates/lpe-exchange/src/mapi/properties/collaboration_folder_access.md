---
type: Rust Function
title: collaboration_folder_access
resource: crates/lpe-exchange/src/mapi/properties.rs#L817-L834
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_rights
  - functions/crates/lpe-exchange/src/mapi/permissions/access_from_rights
  - functions/crates/lpe-exchange/src/mapi/permissions/may_share_from_rights
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
---

# Signature

`fn collaboration_folder_access(folder: &MapiCollaborationFolder) -> u32`

# Calls

- [collaboration_folder_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_rights.md)
- [access_from_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/access_from_rights.md)
- [may_share_from_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/may_share_from_rights.md)

# Called by

- [collaboration_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)