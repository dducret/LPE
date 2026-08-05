---
type: Rust Method
title: query_jmap_email_ids
resource: crates/lpe-jmap/src/tests.rs#L1007-L1032
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/email_matches_visible_search
---

# Signature

`async fn query_jmap_email_ids( &self, _account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&str>, position: u64, limit: u64, ) -> Result<JmapEmailQuery>`

# Calls

- [email_matches_visible_search](../../../../../../../functions/crates/lpe-jmap/src/tests/email_matches_visible_search.md)