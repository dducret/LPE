---
type: Rust Function
title: reserved_folder_counter_for_role
resource: crates/lpe-exchange/src/mapi_store.rs#L1061-L1109
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/email_role_folder_id
  - functions/crates/lpe-exchange/src/mapi_store/is_virtual_special_mailbox
  - functions/crates/lpe-exchange/src/mapi_store/reserved_folder_id_for_role
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests
  - functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id
---

# Signature

`pub(crate) fn reserved_folder_counter_for_role(role: &str) -> Option<u64>`

# Called by

- [email_role_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/email_role_folder_id.md)
- [is_virtual_special_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_store/is_virtual_special_mailbox.md)
- [reserved_folder_id_for_role](../../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_id_for_role.md)
- [mapi_folder_identity_requests](../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests.md)
- [fake_mapi_identity_lookup_for_object_id](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id.md)