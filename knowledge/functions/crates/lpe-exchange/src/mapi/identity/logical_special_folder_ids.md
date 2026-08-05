---
type: Rust Function
title: logical_special_folder_ids
resource: crates/lpe-exchange/src/mapi/identity.rs#L560-L562
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/is_logical_special_folder_id
---

# Signature

`pub(crate) fn logical_special_folder_ids() -> impl Iterator<Item = u64>`

# Called by

- [legacy_for_tests](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [from_special_folder_identity_records](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records.md)
- [is_logical_special_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/is_logical_special_folder_id.md)