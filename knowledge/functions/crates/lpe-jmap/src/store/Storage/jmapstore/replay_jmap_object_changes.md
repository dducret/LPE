---
type: Rust Method
title: replay_jmap_object_changes
resource: crates/lpe-jmap/src/store.rs#L561-L570
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn replay_jmap_object_changes( &self, account_id: Uuid, data_type: &str, after_cursor: i64, max_rows: u64, ) -> Result<Option<Vec<JmapMailObjectChange>>>`