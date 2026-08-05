---
type: Rust Method
title: fetch_delegate_access_objects
resource: crates/lpe-storage/src/collaboration.rs#L49-L115
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-admin-api/src/delegation/get_free_busy
  - functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages
---

# Signature

`pub async fn fetch_delegate_access_objects( &self, principal_account_id: Uuid, ) -> Result<Vec<DelegateAccessObject>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [get_free_busy](../../../../../../functions/crates/lpe-admin-api/src/delegation/get_free_busy.md)
- [compute_delegate_freebusy_messages](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages.md)