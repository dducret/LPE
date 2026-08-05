---
type: Rust Function
title: probe_lpe_core_delivery
resource: LPE-CT/src/dashboard_config.rs#L448-L500
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/lpe_health_probe_url
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/LPE-CT/src/http_routes/test_accepted_domain
---

# Signature

`pub(crate) async fn probe_lpe_core_delivery( core_delivery_base_url: &str, ) -> Result<LpeCoreDeliveryProbe, ApiError>`

# Calls

- [lpe_health_probe_url](../../../../functions/LPE-CT/src/dashboard_config/lpe_health_probe_url.md)
- [build](../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [status](../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [test_accepted_domain](../../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)