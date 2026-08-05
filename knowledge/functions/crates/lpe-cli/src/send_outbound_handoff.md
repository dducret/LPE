---
type: Rust Function
title: send_outbound_handoff
resource: crates/lpe-cli/src/main.rs#L354-L383
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/crates/lpe-cli/src/dispatch_outbound_message
  - functions/crates/lpe-cli/src/handoff_client_posts_json_and_header
---

# Signature

`async fn send_outbound_handoff( client: &reqwest::Client, endpoint: &str, integration_key: &str, trace_id: &str, item: &OutboundMessageHandoffRequest, ) -> std::result::Result<OutboundMessageHandoffResponse, String>`

# Calls

- [sign](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign.md)
- [status](../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [dispatch_outbound_message](../../../../functions/crates/lpe-cli/src/dispatch_outbound_message.md)
- [handoff_client_posts_json_and_header](../../../../functions/crates/lpe-cli/src/handoff_client_posts_json_and_header.md)