---
type: Rust Module
title: imports
resource: crates/lpe-jmap/src/mail/imports.rs#L1-L125
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-magika-expectedkind-ingresscontext-policydecision-validationrequest
  - external/lpe-storage-mail-parse-rfc822-message-authenticatedaccount-jmapimportedemailinput-mailboxaccountaccess
  - external/serde-json-value
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/crate-convert-map-parsed-recipients-parse-parse-uuid-upload-parse-upload-blob-id-jmapservice
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [parse_email_import](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)
- [ensure_target_mailbox_accepts_message_write](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest}`
- `lpe_storage::{
    mail::parse_rfc822_message, AuthenticatedAccount, JmapImportedEmailInput, MailboxAccountAccess,
}`
- `serde_json::Value`
- `std::collections::HashMap`
- `uuid::Uuid`
- `crate::{
    convert::map_parsed_recipients, parse::parse_uuid, upload::parse_upload_blob_id, JmapService,
}`

# Member of

- [lpe-jmap](../../../../../packages/crates/lpe-jmap.md)