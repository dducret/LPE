---
type: Rust Method
title: set_mapi_calendar_permission
resource: crates/lpe-exchange/src/tests/mod.rs#L7324-L7381
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_permission
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
---

# Signature

`fn set_mapi_calendar_permission<'a>( &'a self, owner_account_id: Uuid, grantee_account_id: Uuid, may_read: bool, may_write: bool, may_delete: bool, may_share: bool, audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rights_from_grant](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant.md)
- [owner_permission](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_permission.md)

# Called by

- [append_modify_permissions_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)