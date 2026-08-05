---
type: Rust Method
title: fetch_mailbox_logical_quota_used_octets
resource: crates/lpe-storage/src/jmap_blobs.rs#L65-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/logical_quota_snapshot
---

# Signature

`pub(crate) async fn fetch_mailbox_logical_quota_used_octets( &self, account_id: Uuid, mailbox_id: Uuid, ) -> Result<u64>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)

# Called by

- [logical_quota_snapshot](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/logical_quota_snapshot.md)