---
type: Rust Method
title: save_jmap_query_state
resource: crates/lpe-jmap/src/tests.rs#L927-L953
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/fake_filter_hash
  - functions/crates/lpe-jmap/src/tests/fake_sort_hash
---

# Signature

`async fn save_jmap_query_state( &self, account_id: Uuid, method_name: &str, filter: Option<Value>, sort: Option<Vec<Value>>, last_change_sequence: i64, snapshot_ids: &[String], ) -> Result<Option<Uuid>>`

# Calls

- [fake_filter_hash](../../../../../../../functions/crates/lpe-jmap/src/tests/fake_filter_hash.md)
- [fake_sort_hash](../../../../../../../functions/crates/lpe-jmap/src/tests/fake_sort_hash.md)