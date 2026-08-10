---
type: Rust Function
title: handoff_client_posts_json_and_header
resource: crates/lpe-cli/src/main.rs#L457-L571
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/crates/lpe-cli/src/send_outbound_handoff
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload
---

# Signature

`async fn handoff_client_posts_json_and_header()`

# Calls

- [build](../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [send_outbound_handoff](../../../../functions/crates/lpe-cli/src/send_outbound_handoff.md)
- [validate_payload](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload.md)