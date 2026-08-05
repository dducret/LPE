---
type: Rust Module
title: render
resource: crates/lpe-imap/src/render.rs#L1-L1339
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-mailboxnamepolicy-mailboxpath
  - external/std-collections-hashset
  - external/uuid-uuid
  - external/lpe-storage-imapemail-imapmailboxstate-imapmimepart-jmapemailaddress
  - external/crate-parse-tokenize-messagerefkind-selectedmailbox
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [FetchAttributes](../../../../classes/crates/lpe-imap/src/render/FetchAttributes.md)
- [FetchItem](../../../../classes/crates/lpe-imap/src/render/FetchItem.md)
- [BodySectionFetch](../../../../classes/crates/lpe-imap/src/render/BodySectionFetch.md)
- [PartialRange](../../../../classes/crates/lpe-imap/src/render/PartialRange.md)
- [mailbox_name_matches](../../../../functions/crates/lpe-imap/src/render/mailbox_name_matches.md)
- [render_list_flags](../../../../functions/crates/lpe-imap/src/render/render_list_flags.md)
- [render_mailbox_name](../../../../functions/crates/lpe-imap/src/render/render_mailbox_name.md)
- [render_imap_mailbox_response_path](../../../../functions/crates/lpe-imap/src/render/render_imap_mailbox_response_path.md)
- [render_imap_mailbox_string](../../../../functions/crates/lpe-imap/src/render/render_imap_mailbox_string.md)
- [parse_fetch_attributes](../../../../functions/crates/lpe-imap/src/render/parse_fetch_attributes.md)
- [ensure_uid_fetch_attributes](../../../../functions/crates/lpe-imap/src/render/ensure_uid_fetch_attributes.md)
- [render_fetch_response](../../../../functions/crates/lpe-imap/src/render/render_fetch_response.md)
- [parse_fetch_item_list](../../../../functions/crates/lpe-imap/src/render/parse_fetch_item_list.md)
- [strip_wrapping_parens](../../../../functions/crates/lpe-imap/src/render/strip_wrapping_parens.md)
- [parse_fetch_item](../../../../functions/crates/lpe-imap/src/render/parse_fetch_item.md)
- [parse_body_fetch_item](../../../../functions/crates/lpe-imap/src/render/parse_body_fetch_item.md)
- [parse_partial_range](../../../../functions/crates/lpe-imap/src/render/parse_partial_range.md)
- [fetch_item_marks_seen](../../../../functions/crates/lpe-imap/src/render/fetch_item_marks_seen.md)
- [append_body_section](../../../../functions/crates/lpe-imap/src/render/append_body_section.md)
- [normalize_body_section](../../../../functions/crates/lpe-imap/src/render/normalize_body_section.md)
- [is_header_fields_section](../../../../functions/crates/lpe-imap/src/render/is_header_fields_section.md)
- [section_label](../../../../functions/crates/lpe-imap/src/render/section_label.md)
- [apply_partial](../../../../functions/crates/lpe-imap/src/render/apply_partial.md)
- [append_literal](../../../../functions/crates/lpe-imap/src/render/append_literal.md)
- [render_flags](../../../../functions/crates/lpe-imap/src/render/render_flags.md)
- [imap_keyword_atom](../../../../functions/crates/lpe-imap/src/render/imap_keyword_atom.md)
- [render_status_response](../../../../functions/crates/lpe-imap/src/render/render_status_response.md)
- [render_header](../../../../functions/crates/lpe-imap/src/render/render_header.md)
- [render_header_lines](../../../../functions/crates/lpe-imap/src/render/render_header_lines.md)
- [render_header_fields](../../../../functions/crates/lpe-imap/src/render/render_header_fields.md)
- [render_full_message](../../../../functions/crates/lpe-imap/src/render/render_full_message.md)
- [message_rfc822_size](../../../../functions/crates/lpe-imap/src/render/message_rfc822_size.md)
- [render_message_body](../../../../functions/crates/lpe-imap/src/render/render_message_body.md)
- [render_part_section](../../../../functions/crates/lpe-imap/src/render/render_part_section.md)
- [render_text_part_mime_header](../../../../functions/crates/lpe-imap/src/render/render_text_part_mime_header.md)
- [render_root_mime_header](../../../../functions/crates/lpe-imap/src/render/render_root_mime_header.md)
- [root_content_type](../../../../functions/crates/lpe-imap/src/render/root_content_type.md)
- [render_alternative_body](../../../../functions/crates/lpe-imap/src/render/render_alternative_body.md)
- [render_mixed_body](../../../../functions/crates/lpe-imap/src/render/render_mixed_body.md)
- [multipart_boundary](../../../../functions/crates/lpe-imap/src/render/multipart_boundary.md)
- [mixed_boundary](../../../../functions/crates/lpe-imap/src/render/mixed_boundary.md)
- [render_envelope](../../../../functions/crates/lpe-imap/src/render/render_envelope.md)
- [render_recipients](../../../../functions/crates/lpe-imap/src/render/render_recipients.md)
- [render_address_list](../../../../functions/crates/lpe-imap/src/render/render_address_list.md)
- [render_single_address](../../../../functions/crates/lpe-imap/src/render/render_single_address.md)
- [render_bodystructure](../../../../functions/crates/lpe-imap/src/render/render_bodystructure.md)
- [render_body_bodystructure](../../../../functions/crates/lpe-imap/src/render/render_body_bodystructure.md)
- [render_text_bodystructure](../../../../functions/crates/lpe-imap/src/render/render_text_bodystructure.md)
- [render_attachment_bodystructure](../../../../functions/crates/lpe-imap/src/render/render_attachment_bodystructure.md)
- [render_fallback_attachment_bodystructure](../../../../functions/crates/lpe-imap/src/render/render_fallback_attachment_bodystructure.md)
- [email_has_attachment_parts](../../../../functions/crates/lpe-imap/src/render/email_has_attachment_parts.md)
- [attachment_parts](../../../../functions/crates/lpe-imap/src/render/attachment_parts.md)
- [imap_attachment_part](../../../../functions/crates/lpe-imap/src/render/imap_attachment_part.md)
- [split_content_type](../../../../functions/crates/lpe-imap/src/render/split_content_type.md)
- [imap_media_token](../../../../functions/crates/lpe-imap/src/render/imap_media_token.md)
- [render_content_type_parameters](../../../../functions/crates/lpe-imap/src/render/render_content_type_parameters.md)
- [content_type_parameter](../../../../functions/crates/lpe-imap/src/render/content_type_parameter.md)
- [render_part_mime_header](../../../../functions/crates/lpe-imap/src/render/render_part_mime_header.md)
- [render_attachment_mime_header](../../../../functions/crates/lpe-imap/src/render/render_attachment_mime_header.md)
- [render_disposition](../../../../functions/crates/lpe-imap/src/render/render_disposition.md)
- [ResolvedBodyPart](../../../../classes/crates/lpe-imap/src/render/ResolvedBodyPart.md)
- [resolve_body_part](../../../../functions/crates/lpe-imap/src/render/resolve_body_part.md)
- [body_part_charset](../../../../functions/crates/lpe-imap/src/render/body_part_charset.md)
- [nstring](../../../../functions/crates/lpe-imap/src/render/nstring.md)
- [render_visible_header](../../../../functions/crates/lpe-imap/src/render/render_visible_header.md)
- [render_recipient_header](../../../../functions/crates/lpe-imap/src/render/render_recipient_header.md)
- [render_address_header](../../../../functions/crates/lpe-imap/src/render/render_address_header.md)
- [fallback_address](../../../../functions/crates/lpe-imap/src/render/fallback_address.md)
- [normalized_display_name](../../../../functions/crates/lpe-imap/src/render/normalized_display_name.md)
- [format_internal_date](../../../../functions/crates/lpe-imap/src/render/format_internal_date.md)
- [format_message_date](../../../../functions/crates/lpe-imap/src/render/format_message_date.md)
- [format_rfc5322_date](../../../../functions/crates/lpe-imap/src/render/format_rfc5322_date.md)
- [month_name](../../../../functions/crates/lpe-imap/src/render/month_name.md)
- [resolve_message_indexes](../../../../functions/crates/lpe-imap/src/render/resolve_message_indexes.md)
- [resolve_set_value](../../../../functions/crates/lpe-imap/src/render/resolve_set_value.md)
- [find_message_index](../../../../functions/crates/lpe-imap/src/render/find_message_index.md)
- [parse_status_items](../../../../functions/crates/lpe-imap/src/render/parse_status_items.md)
- [render_modified_set](../../../../functions/crates/lpe-imap/src/render/render_modified_set.md)
- [sanitize_imap_text](../../../../functions/crates/lpe-imap/src/render/sanitize_imap_text.md)
- [sanitize_imap_quoted](../../../../functions/crates/lpe-imap/src/render/sanitize_imap_quoted.md)
- [render_selected_updates](../../../../functions/crates/lpe-imap/src/render/render_selected_updates.md)
- [first_unseen_sequence](../../../../functions/crates/lpe-imap/src/render/first_unseen_sequence.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::{MailboxNamePolicy, MailboxPath}`
- `std::collections::HashSet`
- `uuid::Uuid`
- `lpe_storage::{ImapEmail, ImapMailboxState, ImapMimePart, JmapEmailAddress}`
- `crate::{parse::tokenize, MessageRefKind, SelectedMailbox}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)