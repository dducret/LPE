---
type: Rust Method
title: fetch_jmap_query_state
resource: crates/lpe-jmap/src/tests.rs#L957-L975
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/tests/fake_filter_hash
  - functions/crates/lpe-jmap/src/tests/fake_sort_hash
---

# Signature

`async fn fetch_jmap_query_state( &self, _account_id: Uuid, _method_name: &str, state_id: Uuid, filter: Option<Value>, sort: Option<Vec<Value>>, ) -> Result<Option<JmapStoredQueryState>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [fake_filter_hash](../../../../../../../functions/crates/lpe-jmap/src/tests/fake_filter_hash.md)
- [fake_sort_hash](../../../../../../../functions/crates/lpe-jmap/src/tests/fake_sort_hash.md)