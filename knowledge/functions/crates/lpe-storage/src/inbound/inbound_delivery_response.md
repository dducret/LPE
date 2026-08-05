---
type: Rust Function
title: inbound_delivery_response
resource: crates/lpe-storage/src/inbound.rs#L719-L750
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
  - functions/crates/lpe-storage/src/inbound/inbound_response_rejects_when_no_recipient_was_accepted
  - functions/crates/lpe-storage/src/inbound/inbound_response_accepts_when_at_least_one_recipient_was_accepted
---

# Signature

`fn inbound_delivery_response( accepted: Vec<String>, rejected: Vec<String>, stored_messages: Vec<(Uuid, Uuid)>, followup_errors: Vec<String>, ) -> InboundDeliveryResponse`

# Called by

- [deliver_inbound_message](../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [inbound_response_rejects_when_no_recipient_was_accepted](../../../../../functions/crates/lpe-storage/src/inbound/inbound_response_rejects_when_no_recipient_was_accepted.md)
- [inbound_response_accepts_when_at_least_one_recipient_was_accepted](../../../../../functions/crates/lpe-storage/src/inbound/inbound_response_accepts_when_at_least_one_recipient_was_accepted.md)