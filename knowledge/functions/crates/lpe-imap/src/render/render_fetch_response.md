---
type: Rust Function
title: render_fetch_response
resource: crates/lpe-imap/src/render.rs#L133-L174
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/append_body_section
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
  - functions/crates/lpe-imap/src/render/tests/fetch_envelope_uses_parseable_sender_fallback
  - functions/crates/lpe-imap/src/render/tests/fetch_header_does_not_duplicate_address_as_display_name
  - functions/crates/lpe-imap/src/render/tests/body_peek_fetch_response_uses_body_label
  - functions/crates/lpe-imap/src/render/tests/bodystructure_wraps_alternative_body_in_mixed_when_attachments_exist
---

# Signature

`pub(crate) fn render_fetch_response( sequence: usize, email: &ImapEmail, requested: &FetchAttributes, ) -> Result<Vec<u8>>`

# Calls

- [append_body_section](../../../../../functions/crates/lpe-imap/src/render/append_body_section.md)

# Called by

- [handle_fetch](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)
- [fetch_envelope_uses_parseable_sender_fallback](../../../../../functions/crates/lpe-imap/src/render/tests/fetch_envelope_uses_parseable_sender_fallback.md)
- [fetch_header_does_not_duplicate_address_as_display_name](../../../../../functions/crates/lpe-imap/src/render/tests/fetch_header_does_not_duplicate_address_as_display_name.md)
- [body_peek_fetch_response_uses_body_label](../../../../../functions/crates/lpe-imap/src/render/tests/body_peek_fetch_response_uses_body_label.md)
- [bodystructure_wraps_alternative_body_in_mixed_when_attachments_exist](../../../../../functions/crates/lpe-imap/src/render/tests/bodystructure_wraps_alternative_body_in_mixed_when_attachments_exist.md)