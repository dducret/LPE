---
type: Rust Method
title: query_jmap_email_ids
resource: crates/lpe-storage/src/jmap_queries.rs#L116-L197
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn query_jmap_email_ids( &self, account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&str>, position: u64, limit: u64, ) -> Result<JmapEmailQuery>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)