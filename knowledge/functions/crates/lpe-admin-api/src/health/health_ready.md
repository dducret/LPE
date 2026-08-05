---
type: Rust Function
title: health_ready
resource: crates/lpe-admin-api/src/health.rs#L27-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics
  - functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_check
  - functions/crates/lpe-admin-api/src/readiness/build_readiness_response
---

# Signature

`pub(crate) async fn health_ready(State(storage): State<Storage>) -> ApiResult<ReadinessResponse>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [fetch_storage_metadata_diagnostics](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics.md)
- [storage_metadata_readiness_check](../../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_check.md)
- [build_readiness_response](../../../../../functions/crates/lpe-admin-api/src/readiness/build_readiness_response.md)