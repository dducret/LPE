---
type: Rust Method
title: fetch_jmap_query_state
resource: crates/lpe-jmap/src/store.rs#L604-L614
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn fetch_jmap_query_state( &self, account_id: Uuid, method_name: &str, state_id: Uuid, filter: Option<Value>, sort: Option<Vec<Value>>, ) -> Result<Option<JmapStoredQueryState>>`