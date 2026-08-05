---
type: Rust Function
title: shared_collection_id
resource: crates/lpe-storage/src/collaboration/types.rs#L376-L378
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner
  - functions/crates/lpe-storage/src/collaboration/types/shared_collection_id_for_row
---

# Signature

`fn shared_collection_id(kind: CollaborationResourceKind, owner_account_id: Uuid) -> String`

# Called by

- [collection_id_for_owner](../../../../../../functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner.md)
- [shared_collection_id_for_row](../../../../../../functions/crates/lpe-storage/src/collaboration/types/shared_collection_id_for_row.md)