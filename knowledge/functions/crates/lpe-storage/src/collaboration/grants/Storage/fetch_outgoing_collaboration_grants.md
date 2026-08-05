---
type: Rust Method
title: fetch_outgoing_collaboration_grants
resource: crates/lpe-storage/src/collaboration/grants.rs#L701-L819
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  called_by:
  - functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_shares
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path
---

# Signature

`pub async fn fetch_outgoing_collaboration_grants( &self, owner_account_id: Uuid, kind: CollaborationResourceKind, ) -> Result<Vec<CollaborationGrant>>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)

# Called by

- [list_collaboration_overview](../../../../../../../functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview.md)
- [mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant.md)
- [fetch_jmap_shares](../../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_shares.md)
- [upsert_collaboration_grant](../../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [exercise_custom_calendar_grant_path](../../../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path.md)