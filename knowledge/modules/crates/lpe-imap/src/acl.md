---
type: Rust Module
title: acl
resource: crates/lpe-imap/src/acl.rs#L1-L519
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/lpe-magika-detector
  - external/lpe-storage-auditentryinput-mailboxdelegationgrantinput-senderdelegationgrantinput-senderdelegationright
  - external/std-collections-btreemap-btreeset
  - external/tokio-io-asyncwriteext
  - external/crate-parse-tokenize-render-render-imap-mailbox-string-render-mailbox-name-sanitize-imap-quoted-sanitize-imap-text-session
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [AclState](../../../../classes/crates/lpe-imap/src/acl/AclState.md)
- [handle_getacl](../../../../functions/crates/lpe-imap/src/acl/Session/handle_getacl.md)
- [handle_myrights](../../../../functions/crates/lpe-imap/src/acl/Session/handle_myrights.md)
- [handle_listrights](../../../../functions/crates/lpe-imap/src/acl/Session/handle_listrights.md)
- [handle_setacl](../../../../functions/crates/lpe-imap/src/acl/Session/handle_setacl.md)
- [handle_deleteacl](../../../../functions/crates/lpe-imap/src/acl/Session/handle_deleteacl.md)
- [apply_acl_update](../../../../functions/crates/lpe-imap/src/acl/Session/apply_acl_update.md)
- [combine_acl_state](../../../../functions/crates/lpe-imap/src/acl/combine_acl_state.md)
- [parse_acl_state_update](../../../../functions/crates/lpe-imap/src/acl/parse_acl_state_update.md)
- [parse_acl_rights](../../../../functions/crates/lpe-imap/src/acl/parse_acl_rights.md)
- [render_acl_rights](../../../../functions/crates/lpe-imap/src/acl/render_acl_rights.md)
- [sync_sender_right](../../../../functions/crates/lpe-imap/src/acl/sync_sender_right.md)

# Imports

- `anyhow::{bail, Result}`
- `lpe_magika::Detector`
- `lpe_storage::{
    AuditEntryInput, MailboxDelegationGrantInput, SenderDelegationGrantInput, SenderDelegationRight,
}`
- `std::collections::{BTreeMap, BTreeSet}`
- `tokio::io::AsyncWriteExt`
- `crate::{
    parse::tokenize,
    render::{
        render_imap_mailbox_string, render_mailbox_name, sanitize_imap_quoted, sanitize_imap_text,
    },
    Session,
}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)