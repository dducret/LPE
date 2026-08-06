---
type: Rust Method
title: replay_jmap_string_object_changes
resource: crates/lpe-jmap/src/store.rs#L580-L589
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn replay_jmap_string_object_changes( &self, account_id: Uuid, data_type: &str, after_cursor: i64, max_rows: u64, ) -> Result<Option<Vec<JmapStringObjectChange>>>`