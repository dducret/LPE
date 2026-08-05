---
type: Rust Module
title: search
resource: crates/lpe-imap/src/search.rs#L1-L344
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-storage-imapemail-jmapemailaddress
  - external/crate-parse-tokenize-render-render-address-header-render-recipient-header-render-visible-header-messagerefkind
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [SearchExpression](../../../../classes/crates/lpe-imap/src/search/SearchExpression.md)
- [from_tokens](../../../../functions/crates/lpe-imap/src/search/SearchExpression/from_tokens.md)
- [matches](../../../../functions/crates/lpe-imap/src/search/SearchExpression/matches.md)
- [message_matches_set](../../../../functions/crates/lpe-imap/src/search/message_matches_set.md)
- [resolve_set_value](../../../../functions/crates/lpe-imap/src/search/resolve_set_value.md)
- [parse_search_key](../../../../functions/crates/lpe-imap/src/search/parse_search_key.md)
- [next_search_argument](../../../../functions/crates/lpe-imap/src/search/next_search_argument.md)
- [looks_like_message_set](../../../../functions/crates/lpe-imap/src/search/looks_like_message_set.md)
- [normalize_search_text](../../../../functions/crates/lpe-imap/src/search/normalize_search_text.md)
- [searchable_sender](../../../../functions/crates/lpe-imap/src/search/searchable_sender.md)
- [searchable_recipients](../../../../functions/crates/lpe-imap/src/search/searchable_recipients.md)
- [search_email_text](../../../../functions/crates/lpe-imap/src/search/search_email_text.md)
- [searchable_body](../../../../functions/crates/lpe-imap/src/search/searchable_body.md)
- [searchable_header_value](../../../../functions/crates/lpe-imap/src/search/searchable_header_value.md)
- [parse_search_date](../../../../functions/crates/lpe-imap/src/search/parse_search_date.md)
- [message_search_date](../../../../functions/crates/lpe-imap/src/search/message_search_date.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_storage::{ImapEmail, JmapEmailAddress}`
- `crate::{
    parse::tokenize,
    render::{render_address_header, render_recipient_header, render_visible_header},
    MessageRefKind,
}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)