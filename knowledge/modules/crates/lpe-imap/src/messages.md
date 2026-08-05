---
type: Rust Module
title: messages
resource: crates/lpe-imap/src/messages.rs#L1-L528
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/lpe-magika-detector
  - external/lpe-storage-auditentryinput-jmapmailbox
  - external/tokio-io-asyncwriteext
  - external/tracing-info
  - external/crate-parse-split-two-tokenize-render-ensure-uid-fetch-attributes-parse-fetch-attributes-render-fetch-response-render-flags-render-modified-set-resolve-message-indexes-fetchattributes-fetchitem-search-searchexpression-store-args-parse-flag-list-parse-store-arguments-parse-store-mode-messagerefkind-selectedmailbox-session
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [handle_fetch](../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)
- [handle_store](../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)
- [handle_search](../../../../functions/crates/lpe-imap/src/messages/Session/handle_search.md)
- [handle_copy](../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)
- [handle_move](../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)
- [message_ref_kind_name](../../../../functions/crates/lpe-imap/src/messages/message_ref_kind_name.md)
- [parse_fetch_arguments](../../../../functions/crates/lpe-imap/src/messages/parse_fetch_arguments.md)
- [parse_fetch_modifier](../../../../functions/crates/lpe-imap/src/messages/parse_fetch_modifier.md)
- [ensure_condstore_fetch_attributes](../../../../functions/crates/lpe-imap/src/messages/ensure_condstore_fetch_attributes.md)
- [strip_search_return_options](../../../../functions/crates/lpe-imap/src/messages/strip_search_return_options.md)
- [ensure_copy_allowed](../../../../functions/crates/lpe-imap/src/messages/ensure_copy_allowed.md)
- [ensure_move_allowed](../../../../functions/crates/lpe-imap/src/messages/ensure_move_allowed.md)
- [ensure_store_flags_supported](../../../../functions/crates/lpe-imap/src/messages/ensure_store_flags_supported.md)
- [flag_present](../../../../functions/crates/lpe-imap/src/messages/flag_present.md)

# Imports

- `anyhow::{bail, Result}`
- `lpe_magika::Detector`
- `lpe_storage::{AuditEntryInput, JmapMailbox}`
- `tokio::io::AsyncWriteExt`
- `tracing::info`
- `crate::{
    parse::{split_two, tokenize},
    render::{
        ensure_uid_fetch_attributes, parse_fetch_attributes, render_fetch_response, render_flags,
        render_modified_set, resolve_message_indexes, FetchAttributes, FetchItem,
    },
    search::SearchExpression,
    store_args::{parse_flag_list, parse_store_arguments, parse_store_mode},
    MessageRefKind, SelectedMailbox, Session,
}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)