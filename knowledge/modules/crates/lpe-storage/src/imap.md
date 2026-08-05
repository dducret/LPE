---
type: Rust Module
title: imap
resource: crates/lpe-storage/src/imap.rs#L1-L273
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-hashmap
  - external/anyhow-anyhow-result
  - external/serde-serialize
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-imapemailrow-jmapemailaddress-jmapemailrecipientrow-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [ImapEmail](../../../../classes/crates/lpe-storage/src/imap/ImapEmail.md)
- [ImapMimePart](../../../../classes/crates/lpe-storage/src/imap/ImapMimePart.md)
- [ImapMailboxState](../../../../classes/crates/lpe-storage/src/imap/ImapMailboxState.md)
- [fetch_imap_emails](../../../../functions/crates/lpe-storage/src/imap/Storage/fetch_imap_emails.md)

# Imports

- `std::collections::HashMap`
- `anyhow::{anyhow, Result}`
- `serde::Serialize`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{ImapEmailRow, JmapEmailAddress, JmapEmailRecipientRow, Storage}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)