---
type: Rust Function
title: mapi_folder_id
resource: crates/lpe-exchange/src/mapi_store.rs#L924-L928
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/reserved_folder_id_for_role
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn mapi_folder_id(mailbox: &JmapMailbox) -> u64`

# Calls

- [reserved_folder_id_for_role](../../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_id_for_role.md)
- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)