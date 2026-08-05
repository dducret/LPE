---
type: Rust Module
title: tests
resource: crates/lpe-imap/src/render/tests.rs#L1-L315
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-mailbox-name-matches-render-fetch-response-render-flags-render-list-flags-render-mailbox-name-fetchattributes-fetchitem
  - external/lpe-storage-imapemail-imapmimepart-jmapmailbox
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [reserved_mailbox_name_matching_is_role_bound](../../../../../functions/crates/lpe-imap/src/render/tests/reserved_mailbox_name_matching_is_role_bound.md)
- [special_use_flags_are_role_based_for_localized_names](../../../../../functions/crates/lpe-imap/src/render/tests/special_use_flags_are_role_based_for_localized_names.md)
- [render_flags_projects_atom_safe_keywords](../../../../../functions/crates/lpe-imap/src/render/tests/render_flags_projects_atom_safe_keywords.md)
- [fetch_envelope_uses_parseable_sender_fallback](../../../../../functions/crates/lpe-imap/src/render/tests/fetch_envelope_uses_parseable_sender_fallback.md)
- [fetch_header_does_not_duplicate_address_as_display_name](../../../../../functions/crates/lpe-imap/src/render/tests/fetch_header_does_not_duplicate_address_as_display_name.md)
- [body_peek_fetch_response_uses_body_label](../../../../../functions/crates/lpe-imap/src/render/tests/body_peek_fetch_response_uses_body_label.md)
- [bodystructure_wraps_alternative_body_in_mixed_when_attachments_exist](../../../../../functions/crates/lpe-imap/src/render/tests/bodystructure_wraps_alternative_body_in_mixed_when_attachments_exist.md)

# Imports

- `super::{
    mailbox_name_matches, render_fetch_response, render_flags, render_list_flags,
    render_mailbox_name, FetchAttributes, FetchItem,
}`
- `lpe_storage::{ImapEmail, ImapMimePart, JmapMailbox}`
- `uuid::Uuid`

# Member of

- [lpe-imap](../../../../../packages/crates/lpe-imap.md)