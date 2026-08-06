---
type: Rust Method
title: create_draft
resource: crates/lpe-jmap/src/mail.rs#L1313-L1355
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/drafts/parse_draft_mutation
  - functions/crates/lpe-jmap/src/convert/select_from_addresses
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set
---

# Signature

`pub(crate) async fn create_draft( &self, account: &AuthenticatedAccount, account_access: &MailboxAccountAccess, value: Value, creation_id: &str, ) -> Result<SavedDraftMessage>`

# Calls

- [parse_draft_mutation](../../../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_mutation.md)
- [select_from_addresses](../../../../../../functions/crates/lpe-jmap/src/convert/select_from_addresses.md)

# Called by

- [handle_email_set](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set.md)