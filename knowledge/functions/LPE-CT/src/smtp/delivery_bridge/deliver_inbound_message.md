---
type: Rust Function
title: deliver_inbound_message
resource: LPE-CT/src/smtp/delivery_bridge.rs#L5-L77
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/crates/lpe-magika/src/mime/extract_visible_text
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign
  - functions/LPE-CT/src/host_logs/HostLogError/status
---

# Signature

`pub(in crate::smtp) async fn deliver_inbound_message( config: &RuntimeConfig, message: &QueuedMessage, ) -> Result<InboundDeliveryResponse>`

# Calls

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [extract_visible_text](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_text.md)
- [build](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [sign](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)