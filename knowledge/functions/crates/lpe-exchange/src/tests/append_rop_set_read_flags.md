---
type: Rust Function
title: append_rop_set_read_flags
resource: crates/lpe-exchange/src/tests/mod.rs#L15410-L15416
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_set_read_flags_updates_canonical_read_state
---

# Signature

`fn append_rop_set_read_flags(rops: &mut Vec<u8>, input: u8, read_flags: u8, message_ids: &[u64])`

# Calls

- [append_mapi_wire_id](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)

# Called by

- [mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end.md)
- [mapi_over_http_public_folder_set_read_flags_updates_canonical_read_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_set_read_flags_updates_canonical_read_state.md)