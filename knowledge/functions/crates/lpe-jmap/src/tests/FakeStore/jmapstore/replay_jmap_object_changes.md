---
type: Rust Method
title: replay_jmap_object_changes
resource: crates/lpe-jmap/src/tests.rs#L907-L915
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn replay_jmap_object_changes( &self, _account_id: Uuid, _data_type: &str, _after_cursor: i64, _max_rows: u64, ) -> Result<Option<Vec<JmapMailObjectChange>>>`