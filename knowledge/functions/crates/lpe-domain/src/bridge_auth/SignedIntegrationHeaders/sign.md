---
type: Rust Method
title: sign
resource: crates/lpe-domain/src/bridge_auth.rs#L55-L69
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce
  called_by:
  - functions/LPE-CT/src/dashboard_config/probe_lpe_recipient_bridge
  - functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message
  - functions/LPE-CT/src/submission/authenticate_smtp_client
  - functions/LPE-CT/src/submission/submit_message
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
  - functions/crates/lpe-cli/src/send_outbound_handoff
---

# Signature

`pub fn sign<T: Serialize>( shared_secret: &str, method: &str, path: &str, payload: &T, ) -> Result<Self, BridgeAuthError>`

# Calls

- [sign_with_timestamp_and_nonce](../../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce.md)

# Called by

- [probe_lpe_recipient_bridge](../../../../../../functions/LPE-CT/src/dashboard_config/probe_lpe_recipient_bridge.md)
- [deliver_inbound_message](../../../../../../functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message.md)
- [authenticate_smtp_client](../../../../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)
- [submit_message](../../../../../../functions/LPE-CT/src/submission/submit_message.md)
- [verify_recipient_with_core](../../../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)
- [send_outbound_handoff](../../../../../../functions/crates/lpe-cli/src/send_outbound_handoff.md)