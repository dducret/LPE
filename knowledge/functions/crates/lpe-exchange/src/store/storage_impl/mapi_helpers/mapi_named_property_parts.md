---
type: Rust Function
title: mapi_named_property_parts
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L280-L287
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/fetch_mapi_named_property_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/insert_mapi_named_property_in_tx
---

# Signature

`fn mapi_named_property_parts( property: &MapiNamedProperty, ) -> (&'static str, Option<i32>, Option<&str>)`

# Called by

- [fetch_mapi_named_property_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/fetch_mapi_named_property_in_tx.md)
- [insert_mapi_named_property_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/insert_mapi_named_property_in_tx.md)