---
type: Rust Method
title: fetch_domain_logical_quota_used_octets
resource: crates/lpe-storage/src/jmap_blobs.rs#L94-L122
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/logical_quota_snapshot
---

# Signature

`pub(crate) async fn fetch_domain_logical_quota_used_octets( &self, tenant_id: &Uuid, domain_id: Uuid, ) -> Result<u64>`

# Called by

- [logical_quota_snapshot](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/logical_quota_snapshot.md)