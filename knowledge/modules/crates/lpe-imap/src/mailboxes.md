---
type: Rust Module
title: mailboxes
resource: crates/lpe-imap/src/mailboxes.rs#L1-L777
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-mailboxnamepolicy
  - external/lpe-magika-detector
  - external/lpe-storage-auditentryinput-jmapmailbox
  - external/tokio-io-asyncwriteext
  - external/tracing-info
  - external/crate-parse-parse-mailbox-path-parse-mailbox-path-token-tokenize-render-first-unseen-sequence-mailbox-name-matches-parse-status-items-render-imap-mailbox-response-path-render-list-flags-render-status-response-resolve-message-indexes-messagerefkind-selectedmailbox-session
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [handle_list](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_list.md)
- [handle_xlist](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_xlist.md)
- [handle_mailbox_listing](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing.md)
- [handle_lsub](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub.md)
- [handle_subscribe](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_subscribe.md)
- [handle_unsubscribe](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_unsubscribe.md)
- [handle_namespace](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_namespace.md)
- [handle_status](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_status.md)
- [handle_create](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_create.md)
- [handle_delete](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_delete.md)
- [handle_rename](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_rename.md)
- [handle_select](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_select.md)
- [handle_examine](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_examine.md)
- [handle_select_mode](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode.md)
- [handle_check](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_check.md)
- [handle_close](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_close.md)
- [handle_unselect](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_unselect.md)
- [handle_expunge](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge.md)
- [handle_uid_expunge](../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge.md)
- [expunge_selected_indices](../../../../functions/crates/lpe-imap/src/mailboxes/Session/expunge_selected_indices.md)
- [delete_selected_indices](../../../../functions/crates/lpe-imap/src/mailboxes/Session/delete_selected_indices.md)
- [resolve_mailbox_by_name](../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)
- [resolve_mailbox_name](../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_name.md)
- [resolve_mailbox_path](../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path.md)
- [mailbox_matches_path](../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path.md)
- [parse_list_pattern](../../../../functions/crates/lpe-imap/src/mailboxes/parse_list_pattern.md)
- [parse_select_mailbox_path](../../../../functions/crates/lpe-imap/src/mailboxes/parse_select_mailbox_path.md)
- [mailbox_pattern_matches](../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_pattern_matches.md)
- [mailbox_matches_pattern](../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern.md)
- [render_mailbox_path](../../../../functions/crates/lpe-imap/src/mailboxes/render_mailbox_path.md)
- [special_mailbox_aliases](../../../../functions/crates/lpe-imap/src/mailboxes/special_mailbox_aliases.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::MailboxNamePolicy`
- `lpe_magika::Detector`
- `lpe_storage::{AuditEntryInput, JmapMailbox}`
- `tokio::io::AsyncWriteExt`
- `tracing::info`
- `crate::{
    parse::{parse_mailbox_path, parse_mailbox_path_token, tokenize},
    render::{
        first_unseen_sequence, mailbox_name_matches, parse_status_items,
        render_imap_mailbox_response_path, render_list_flags, render_status_response,
        resolve_message_indexes,
    },
    MessageRefKind, SelectedMailbox, Session,
}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)