---
type: Rust Method
title: submit_draft_message
resource: crates/lpe-storage/src/submission.rs#L1024-L1120
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn submit_draft_message( &self, account_id: Uuid, draft_message_id: Uuid, submitted_by_account_id: Uuid, source: &str, audit: AuditEntryInput, ) -> Result<SubmittedMessage>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)