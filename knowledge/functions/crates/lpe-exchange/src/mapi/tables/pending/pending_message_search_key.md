---
type: Rust Function
title: pending_message_search_key
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L446-L457
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value
---

# Signature

`fn pending_message_search_key(properties: &HashMap<u32, MapiValue>) -> Vec<u8>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [pending_message_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_change_number.md)

# Called by

- [pending_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value.md)