---
type: Rust Function
title: realistic_imported_identity
resource: crates/lpe-storage/src/mapi_events/imported_identity.rs#L155-L167
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_rejects_a_foreign_source_key_replica
---

# Signature

`fn realistic_imported_identity() -> MapiEventImportedIdentity`

# Calls

- [mapi_change_key](../../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)

# Called by

- [imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key](../../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key.md)
- [imported_identity_rejects_a_foreign_source_key_replica](../../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_rejects_a_foreign_source_key_replica.md)