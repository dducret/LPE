---
type: Rust Function
title: existing_inbound_delivery_response_in_tx
resource: crates/lpe-storage/src/inbound.rs#L642-L704
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/inbound/duplicate_inbound_delivery_response
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
---

# Signature

`async fn existing_inbound_delivery_response_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, trace_id: &str, ) -> Result<Option<InboundDeliveryResponse>>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [duplicate_inbound_delivery_response](../../../../../functions/crates/lpe-storage/src/inbound/duplicate_inbound_delivery_response.md)

# Called by

- [deliver_inbound_message](../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)