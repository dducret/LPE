---
type: Rust Function
title: require_integration_request
resource: LPE-CT/src/management_auth.rs#L65-L106
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload
  called_by:
  - functions/LPE-CT/src/http_routes/outbound_handoff
  - functions/LPE-CT/src/signed_integration_requests_reject_replay
---

# Signature

`pub(crate) fn require_integration_request<T: serde::Serialize>( headers: &HeaderMap, path: &str, payload: &T, ) -> Result<(), ApiError>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [validate_payload](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload.md)

# Called by

- [outbound_handoff](../../../../functions/LPE-CT/src/http_routes/outbound_handoff.md)
- [signed_integration_requests_reject_replay](../../../../functions/LPE-CT/src/signed_integration_requests_reject_replay.md)