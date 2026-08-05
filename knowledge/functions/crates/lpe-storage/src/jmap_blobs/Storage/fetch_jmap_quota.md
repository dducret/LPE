---
type: Rust Method
title: fetch_jmap_quota
resource: crates/lpe-storage/src/jmap_blobs.rs#L26-L62
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_jmap_quota(&self, account_id: Uuid) -> Result<JmapQuota>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)