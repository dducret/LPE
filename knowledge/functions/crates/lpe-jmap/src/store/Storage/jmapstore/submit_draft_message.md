---
type: Rust Method
title: submit_draft_message
resource: crates/lpe-jmap/src/store.rs#L852-L868
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn submit_draft_message( &self, account_id: Uuid, draft_message_id: Uuid, submitted_by_account_id: Uuid, source: &str, audit: AuditEntryInput, ) -> Result<SubmittedMessage>`