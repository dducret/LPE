---
type: Rust Method
title: upsert_mapi_folder_profile_property_values
resource: crates/lpe-exchange/src/tests/mod.rs#L7201-L7219
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/persist_profile_folder_property_values
---

# Signature

`fn upsert_mapi_folder_profile_property_values<'a>( &'a self, account_id: Uuid, values: &'a [MapiFolderProfilePropertyValue], ) -> StoreFuture<'a, ()>`

# Called by

- [persist_profile_folder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/persist_profile_folder_property_values.md)