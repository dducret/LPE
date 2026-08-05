---
type: Rust Method
title: query_jmap_thread_ids
resource: crates/lpe-storage/src/jmap_queries.rs#L249-L332
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn query_jmap_thread_ids( &self, account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&str>, position: u64, limit: u64, ) -> Result<JmapThreadQuery>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)