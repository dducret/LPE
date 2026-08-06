---
type: Rust Method
title: dismiss_jmap_reminder_occurrence
resource: crates/lpe-jmap/src/store.rs#L1238-L1254
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn dismiss_jmap_reminder_occurrence( &self, account_id: Uuid, source_type: String, source_id: Uuid, occurrence_start_at: String, dismissed_at: String, ) -> Result<()>`