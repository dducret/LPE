---
type: Rust Method
title: query_jmap_thread_ids
resource: crates/lpe-jmap/src/tests.rs#L1048-L1077
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/email_matches_visible_search
---

# Signature

`async fn query_jmap_thread_ids( &self, _account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&str>, position: u64, limit: u64, ) -> Result<lpe_storage::JmapThreadQuery>`

# Calls

- [email_matches_visible_search](../../../../../../../functions/crates/lpe-jmap/src/tests/email_matches_visible_search.md)