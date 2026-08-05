---
type: Rust Function
title: object_id_matches
resource: crates/lpe-exchange/src/mapi/identity.rs#L715-L718
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches
---

# Signature

`pub(crate) fn object_id_matches(canonical_id: &Uuid, object_id: u64) -> bool`

# Calls

- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [legacy_migration_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)

# Called by

- [mapi_item_id_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches.md)