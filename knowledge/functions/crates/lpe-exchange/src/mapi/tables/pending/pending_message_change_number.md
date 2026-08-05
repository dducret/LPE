---
type: Rust Function
title: pending_message_change_number
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L459-L481
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_search_key
---

# Signature

`fn pending_message_change_number(properties: &HashMap<u32, MapiValue>) -> u64`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [pending_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value.md)
- [pending_message_search_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_search_key.md)