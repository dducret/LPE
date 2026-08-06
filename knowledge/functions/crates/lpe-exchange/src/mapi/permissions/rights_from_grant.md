---
type: Rust Function
title: rights_from_grant
resource: crates/lpe-exchange/src/mapi/permissions.rs#L58-L80
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_rights
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_rights
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/tests/collaboration_folder_tables_follow_effective_collection_grants
  - functions/crates/lpe-exchange/src/mapi_store/mapi_public_folder_permission
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_modify_permissions_maps_acl_rows_to_canonical_grants
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_maps_acl_rows_to_calendar_grants
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_custom_calendar_modify_permissions_maps_acl_rows_to_calendar_grants
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_modify_permissions_writes_canonical_grants
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_modify_permissions_requires_share_right
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_modify_permissions_rejects_unknown_member_without_grant
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_folder_permission
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_permission
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_collection_permission
---

# Signature

`pub(crate) fn rights_from_grant( may_read: bool, may_write: bool, may_delete: bool, may_share: bool, ) -> u32`

# Called by

- [owner_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_rights.md)
- [collaboration_folder_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_rights.md)
- [public_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [collaboration_folder_tables_follow_effective_collection_grants](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/collaboration_folder_tables_follow_effective_collection_grants.md)
- [mapi_public_folder_permission](../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_public_folder_permission.md)
- [folder_access_for_principal](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant.md)
- [mapi_over_http_modify_permissions_maps_acl_rows_to_canonical_grants](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_modify_permissions_maps_acl_rows_to_canonical_grants.md)
- [mapi_over_http_calendar_modify_permissions_maps_acl_rows_to_calendar_grants](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_maps_acl_rows_to_calendar_grants.md)
- [mapi_over_http_custom_calendar_modify_permissions_maps_acl_rows_to_calendar_grants](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_custom_calendar_modify_permissions_maps_acl_rows_to_calendar_grants.md)
- [mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants.md)
- [mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions.md)
- [mapi_over_http_public_folder_modify_permissions_writes_canonical_grants](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_modify_permissions_writes_canonical_grants.md)
- [mapi_over_http_public_folder_modify_permissions_requires_share_right](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_modify_permissions_requires_share_right.md)
- [mapi_over_http_public_folder_modify_permissions_rejects_unknown_member_without_grant](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_modify_permissions_rejects_unknown_member_without_grant.md)
- [set_mapi_folder_permission](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_folder_permission.md)
- [set_mapi_calendar_permission](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_permission.md)
- [set_mapi_calendar_collection_permission](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_collection_permission.md)