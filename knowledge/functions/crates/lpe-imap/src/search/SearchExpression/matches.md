---
type: Rust Method
title: matches
resource: crates/lpe-imap/src/search.rs#L57-L114
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/search/normalize_search_text
  - functions/crates/lpe-imap/src/search/search_email_text
  - functions/crates/lpe-imap/src/search/searchable_sender
  - functions/crates/lpe-imap/src/search/searchable_recipients
  - functions/crates/lpe-imap/src/search/searchable_body
  - functions/crates/lpe-imap/src/search/searchable_header_value
  - functions/crates/lpe-imap/src/search/message_search_date
  - functions/crates/lpe-imap/src/search/message_matches_set
---

# Signature

`pub(crate) fn matches( &self, email: &ImapEmail, index: usize, emails: &[ImapEmail], ref_kind: MessageRefKind, ) -> Result<bool>`

# Calls

- [normalize_search_text](../../../../../../functions/crates/lpe-imap/src/search/normalize_search_text.md)
- [search_email_text](../../../../../../functions/crates/lpe-imap/src/search/search_email_text.md)
- [searchable_sender](../../../../../../functions/crates/lpe-imap/src/search/searchable_sender.md)
- [searchable_recipients](../../../../../../functions/crates/lpe-imap/src/search/searchable_recipients.md)
- [searchable_body](../../../../../../functions/crates/lpe-imap/src/search/searchable_body.md)
- [searchable_header_value](../../../../../../functions/crates/lpe-imap/src/search/searchable_header_value.md)
- [message_search_date](../../../../../../functions/crates/lpe-imap/src/search/message_search_date.md)
- [message_matches_set](../../../../../../functions/crates/lpe-imap/src/search/message_matches_set.md)