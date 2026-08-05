---
type: Rust Function
title: shared_collection_display_name
resource: crates/lpe-storage/src/collaboration/types.rs#L391-L402
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections
---

# Signature

`pub(super) fn shared_collection_display_name( kind: CollaborationResourceKind, owner_display_name: &str, owner_email: &str, ) -> String`

# Called by

- [fetch_accessible_collections](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections.md)