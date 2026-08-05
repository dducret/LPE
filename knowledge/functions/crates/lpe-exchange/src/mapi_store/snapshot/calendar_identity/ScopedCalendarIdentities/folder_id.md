---
type: Rust Method
title: folder_id
resource: crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity.rs#L48-L63
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn folder_id(&self, collection: &CollaborationCollection) -> Result<u64>`

# Calls

- [collaboration_folder_identity_canonical_id_for_collection](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection.md)
- [expect](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)