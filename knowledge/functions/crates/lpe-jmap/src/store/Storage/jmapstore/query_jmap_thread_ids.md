---
type: Rust Method
title: query_jmap_thread_ids
resource: crates/lpe-jmap/src/store.rs#L706-L716
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn query_jmap_thread_ids( &self, account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&str>, position: u64, limit: u64, ) -> Result<JmapThreadQuery>`