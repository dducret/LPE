---
type: Rust Method
title: list_recoverable_items
resource: crates/lpe-admin-api/src/workspace.rs#L260-L266
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn list_recoverable_items( &self, account_id: Uuid, recoverable_folder: Option<&str>, ) -> anyhow::Result<Vec<RecoverableItem>>`