---
type: Rust Function
title: canonical_message_id_from_ews_id
resource: crates/lpe-exchange/src/service/ews/ids.rs#L55-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/play_on_phone
---

# Signature

`pub(in crate::service) fn canonical_message_id_from_ews_id(id: &str) -> Option<Uuid>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [send_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item.md)
- [play_on_phone](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/play_on_phone.md)