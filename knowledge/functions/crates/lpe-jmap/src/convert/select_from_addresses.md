---
type: Rust Function
title: select_from_addresses
resource: crates/lpe-jmap/src/convert.rs#L135-L181
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/create_draft
  - functions/crates/lpe-jmap/src/mail/JmapService/update_draft
---

# Signature

`pub(crate) fn select_from_addresses( from: Option<Vec<EmailAddressInput>>, sender: Option<Vec<EmailAddressInput>>, account: &AuthenticatedAccount, account_access: &MailboxAccountAccess, ) -> Result<(EmailAddressInput, Option<EmailAddressInput>)>`

# Calls

- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [create_draft](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/create_draft.md)
- [update_draft](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/update_draft.md)