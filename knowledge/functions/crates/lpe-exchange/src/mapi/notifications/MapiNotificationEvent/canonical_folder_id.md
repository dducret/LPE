---
type: Rust Method
title: canonical_folder_id
resource: crates/lpe-exchange/src/mapi/notifications.rs#L216-L218
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response
---

# Signature

`pub(crate) fn canonical_folder_id(&self) -> Option<uuid::Uuid>`

# Called by

- [durable_events_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response.md)