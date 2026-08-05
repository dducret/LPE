---
type: Rust Method
title: folder_entry_id_with_provider
resource: crates/lpe-exchange/src/mapi/identity.rs#L361-L377
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
---

# Signature

`fn folder_entry_id_with_provider( &self, provider_uid: [u8; 16], object_id: u64, entry_type: u16, ) -> Option<Vec<u8>>`

# Calls

- [actual_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)