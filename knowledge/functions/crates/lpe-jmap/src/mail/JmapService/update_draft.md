---
type: Rust Method
title: update_draft
resource: crates/lpe-jmap/src/mail.rs#L1357-L1432
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-jmap/src/drafts/parse_draft_mutation
  - functions/crates/lpe-jmap/src/convert/select_from_addresses
  - functions/crates/lpe-jmap/src/convert/map_existing_recipients
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set
---

# Signature

`pub(crate) async fn update_draft( &self, account: &AuthenticatedAccount, account_access: &MailboxAccountAccess, id: &str, value: Value, ) -> Result<SavedDraftMessage>`

# Calls

- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [parse_draft_mutation](../../../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_mutation.md)
- [select_from_addresses](../../../../../../functions/crates/lpe-jmap/src/convert/select_from_addresses.md)
- [map_existing_recipients](../../../../../../functions/crates/lpe-jmap/src/convert/map_existing_recipients.md)

# Called by

- [handle_email_set](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set.md)