---
type: Rust Function
title: collaboration_folder_rights
resource: crates/lpe-exchange/src/mapi/properties.rs#L804-L815
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_rights
  - functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_access
---

# Signature

`fn collaboration_folder_rights(folder: &MapiCollaborationFolder) -> u32`

# Calls

- [owner_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_rights.md)
- [rights_from_grant](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant.md)

# Called by

- [collaboration_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)
- [collaboration_folder_access](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_access.md)