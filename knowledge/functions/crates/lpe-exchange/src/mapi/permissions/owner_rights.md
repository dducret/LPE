---
type: Rust Function
title: owner_rights
resource: crates/lpe-exchange/src/mapi/permissions.rs#L82-L84
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant
  called_by:
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_permission
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_rights
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/tests/collaboration_folder_tables_follow_effective_collection_grants
---

# Signature

`pub(in crate::mapi) fn owner_rights() -> u32`

# Calls

- [rights_from_grant](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant.md)

# Called by

- [owner_permission](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_permission.md)
- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [collaboration_folder_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_rights.md)
- [search_folder_definition_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value.md)
- [special_folder_property_value_with_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)
- [collaboration_folder_tables_follow_effective_collection_grants](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/collaboration_folder_tables_follow_effective_collection_grants.md)