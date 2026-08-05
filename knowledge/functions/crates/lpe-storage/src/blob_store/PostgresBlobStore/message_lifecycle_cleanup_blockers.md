---
type: Rust Method
title: message_lifecycle_cleanup_blockers
resource: crates/lpe-storage/src/blob_store.rs#L1347-L1422
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility
---

# Signature

`async fn message_lifecycle_cleanup_blockers( &self, pool: &PgPool, tenant_id: Uuid, domain_id: Uuid, blob_id: Uuid, ) -> Result<Vec<String>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [old_placement_cleanup_eligibility](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility.md)