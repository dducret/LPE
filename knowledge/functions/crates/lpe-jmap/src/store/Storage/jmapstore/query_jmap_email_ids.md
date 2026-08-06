---
type: Rust Method
title: query_jmap_email_ids
resource: crates/lpe-jmap/src/store.rs#L686-L696
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn query_jmap_email_ids( &self, account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&str>, position: u64, limit: u64, ) -> Result<JmapEmailQuery>`