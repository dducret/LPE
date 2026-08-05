---
type: Rust Function
title: shared_collection_id_for_row
resource: crates/lpe-storage/src/collaboration/types.rs#L380-L389
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/types/shared_collection_id
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections
---

# Signature

`pub(super) fn shared_collection_id_for_row( kind: CollaborationResourceKind, row: &CollaborationCollectionRow, ) -> String`

# Calls

- [shared_collection_id](../../../../../../functions/crates/lpe-storage/src/collaboration/types/shared_collection_id.md)

# Called by

- [fetch_accessible_collections](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections.md)