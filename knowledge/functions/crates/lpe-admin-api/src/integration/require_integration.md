---
type: Rust Function
title: require_integration
resource: crates/lpe-admin-api/src/integration.rs#L504-L548
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload
  called_by:
  - functions/crates/lpe-admin-api/src/integration/deliver_inbound_message
  - functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient
  - functions/crates/lpe-admin-api/src/integration/authenticate_smtp_submission
  - functions/crates/lpe-admin-api/src/integration/accept_smtp_submission
  - functions/crates/lpe-admin-api/src/integration/integration_requests_require_signed_headers_and_reject_replay
---

# Signature

`fn require_integration<T: serde::Serialize>( headers: &HeaderMap, path: &str, payload: &T, ) -> std::result::Result<(), (StatusCode, String)>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [validate_payload](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload.md)

# Called by

- [deliver_inbound_message](../../../../../functions/crates/lpe-admin-api/src/integration/deliver_inbound_message.md)
- [verify_lpe_ct_recipient](../../../../../functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient.md)
- [authenticate_smtp_submission](../../../../../functions/crates/lpe-admin-api/src/integration/authenticate_smtp_submission.md)
- [accept_smtp_submission](../../../../../functions/crates/lpe-admin-api/src/integration/accept_smtp_submission.md)
- [integration_requests_require_signed_headers_and_reject_replay](../../../../../functions/crates/lpe-admin-api/src/integration/integration_requests_require_signed_headers_and_reject_replay.md)