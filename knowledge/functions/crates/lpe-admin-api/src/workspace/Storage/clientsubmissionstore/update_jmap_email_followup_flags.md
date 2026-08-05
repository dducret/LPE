---
type: Rust Method
title: update_jmap_email_followup_flags
resource: crates/lpe-admin-api/src/workspace.rs#L158-L166
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn update_jmap_email_followup_flags( &self, account_id: Uuid, message_id: Uuid, update: JmapEmailFollowupUpdate, audit: AuditEntryInput, ) -> anyhow::Result<JmapEmail>`