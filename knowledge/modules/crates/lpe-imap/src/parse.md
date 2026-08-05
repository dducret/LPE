---
type: Rust Module
title: parse
resource: crates/lpe-imap/src/parse.rs#L1-L128
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-mailboxnamepolicy-mailboxpath
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [RequestLine](../../../../classes/crates/lpe-imap/src/parse/RequestLine.md)
- [parse_request_line](../../../../functions/crates/lpe-imap/src/parse/parse_request_line.md)
- [tokenize](../../../../functions/crates/lpe-imap/src/parse/tokenize.md)
- [split_two](../../../../functions/crates/lpe-imap/src/parse/split_two.md)
- [parse_literal_size](../../../../functions/crates/lpe-imap/src/parse/parse_literal_size.md)
- [first_token](../../../../functions/crates/lpe-imap/src/parse/first_token.md)
- [parse_mailbox_path_token](../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path_token.md)
- [parse_mailbox_path](../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::{MailboxNamePolicy, MailboxPath}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)