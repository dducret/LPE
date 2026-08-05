---
type: Rust Function
title: duplicate_inbound_delivery_response
resource: crates/lpe-storage/src/inbound.rs#L706-L717
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/inbound/existing_inbound_delivery_response_in_tx
  - functions/crates/lpe-storage/src/inbound/duplicate_inbound_response_returns_committed_receipt
---

# Signature

`fn duplicate_inbound_delivery_response( trace_id: &str, delivered_mailboxes: Vec<String>, ) -> InboundDeliveryResponse`

# Called by

- [existing_inbound_delivery_response_in_tx](../../../../../functions/crates/lpe-storage/src/inbound/existing_inbound_delivery_response_in_tx.md)
- [duplicate_inbound_response_returns_committed_receipt](../../../../../functions/crates/lpe-storage/src/inbound/duplicate_inbound_response_returns_committed_receipt.md)