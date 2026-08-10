---
type: Rust Module
title: transport
resource: crates/lpe-domain/src/transport.rs#L1-L122
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/serde-deserialize-serialize
  - external/uuid-uuid
  - external/crate-encoding-base64-bytes
  member_of:
  - packages/crates/lpe-domain
---

# Contains

- [TransportDeliveryStatus](../../../../classes/crates/lpe-domain/src/transport/TransportDeliveryStatus.md)
- [as_str](../../../../functions/crates/lpe-domain/src/transport/TransportDeliveryStatus/as_str.md)
- [TransportRetryAdvice](../../../../classes/crates/lpe-domain/src/transport/TransportRetryAdvice.md)
- [TransportDsnReport](../../../../classes/crates/lpe-domain/src/transport/TransportDsnReport.md)
- [TransportTechnicalStatus](../../../../classes/crates/lpe-domain/src/transport/TransportTechnicalStatus.md)
- [TransportRouteDecision](../../../../classes/crates/lpe-domain/src/transport/TransportRouteDecision.md)
- [TransportThrottleStatus](../../../../classes/crates/lpe-domain/src/transport/TransportThrottleStatus.md)
- [TransportRecipient](../../../../classes/crates/lpe-domain/src/transport/TransportRecipient.md)
- [OutboundMessageHandoffRequest](../../../../classes/crates/lpe-domain/src/transport/OutboundMessageHandoffRequest.md)
- [envelope_recipients](../../../../functions/crates/lpe-domain/src/transport/OutboundMessageHandoffRequest/envelope_recipients.md)
- [OutboundMessageHandoffResponse](../../../../classes/crates/lpe-domain/src/transport/OutboundMessageHandoffResponse.md)

# Imports

- `serde::{Deserialize, Serialize}`
- `uuid::Uuid`
- `crate::encoding::base64_bytes`

# Member of

- [lpe-domain](../../../../packages/crates/lpe-domain.md)