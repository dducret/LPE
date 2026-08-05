---
type: Rust Function
title: check_optional_http_dependency
resource: crates/lpe-admin-api/src/readiness.rs#L120-L147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/src/host_logs/HostLogError/status
---

# Signature

`pub(crate) async fn check_optional_http_dependency( name: &str, url: &str, ok_detail: &str, warn_detail: &str, ) -> ReadinessCheck`

# Calls

- [build](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)