---
type: Rust Function
title: probe_lpe_recipient_bridge
resource: LPE-CT/src/dashboard_config.rs#L516-L608
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/lpe_bridge_probe_url
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/LPE-CT/src/http_routes/test_accepted_domain
---

# Signature

`pub(crate) async fn probe_lpe_recipient_bridge( core_delivery_base_url: &str, domain: &str, ) -> Result<LpeRecipientBridgeProbe, ApiError>`

# Calls

- [lpe_bridge_probe_url](../../../../functions/LPE-CT/src/dashboard_config/lpe_bridge_probe_url.md)
- [sign](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign.md)
- [build](../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [status](../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [test_accepted_domain](../../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)