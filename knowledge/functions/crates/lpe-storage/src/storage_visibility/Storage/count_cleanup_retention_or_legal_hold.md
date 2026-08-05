---
type: Rust Method
title: count_cleanup_retention_or_legal_hold
resource: crates/lpe-storage/src/storage_visibility.rs#L549-L596
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_counts
---

# Signature

`async fn count_cleanup_retention_or_legal_hold( &self, tenant_filter: Option<Uuid>, ) -> Result<u64>`

# Called by

- [load_cleanup_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_counts.md)