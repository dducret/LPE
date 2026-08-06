---
type: Rust Function
title: strict_decode_change_number_property
resource: crates/lpe-exchange/src/tests/mod.rs#L13459-L13469
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_decode_object_id_property
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_record_content_header_property
---

# Signature

`fn strict_decode_change_number_property( property: &StrictFastTransferProperty, ) -> Result<u64, String>`

# Calls

- [strict_decode_object_id_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_object_id_property.md)
- [global_counter_from_store_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [strict_record_content_header_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_content_header_property.md)