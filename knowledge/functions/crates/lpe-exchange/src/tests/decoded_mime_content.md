---
type: Rust Function
title: decoded_mime_content
resource: crates/lpe-exchange/src/tests/mod.rs#L12404-L12411
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/get_item_returns_requested_mime_content_without_leaking_bcc_for_normal_mailbox
  - functions/crates/lpe-exchange/src/tests/ews/get_item_mime_content_hides_bcc_for_sent_message_default_fetch
  - functions/crates/lpe-exchange/src/tests/ews/get_item_mime_content_includes_canonical_attachments
---

# Signature

`fn decoded_mime_content(response: &str) -> String`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [get_item_returns_requested_mime_content_without_leaking_bcc_for_normal_mailbox](../../../../../functions/crates/lpe-exchange/src/tests/ews/get_item_returns_requested_mime_content_without_leaking_bcc_for_normal_mailbox.md)
- [get_item_mime_content_hides_bcc_for_sent_message_default_fetch](../../../../../functions/crates/lpe-exchange/src/tests/ews/get_item_mime_content_hides_bcc_for_sent_message_default_fetch.md)
- [get_item_mime_content_includes_canonical_attachments](../../../../../functions/crates/lpe-exchange/src/tests/ews/get_item_mime_content_includes_canonical_attachments.md)