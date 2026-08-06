---
type: Rust Method
title: save_jmap_query_state
resource: crates/lpe-jmap/src/store.rs#L591-L610
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn save_jmap_query_state( &self, account_id: Uuid, method_name: &str, filter: Option<Value>, sort: Option<Vec<Value>>, last_change_sequence: i64, snapshot_ids: &[String], ) -> Result<Option<Uuid>>`