---
type: Rust Module
title: tests
resource: crates/lpe-domain/src/tests.rs#L1-L322
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-mailboxdisplayname-mailboxnameerror-mailboxnamepolicy-mailboxpath-mailboxsegment-outboundmessagehandoffrequest-outboundmessagehandoffresponse-smtpsubmissionrequest-transportdeliverystatus-transportdsnreport-transportrecipient-transportretryadvice-transportroutedecision-transporttechnicalstatus-transportthrottlestatus
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-domain
---

# Contains

- [transport_delivery_status_serializes_as_lowercase](../../../../functions/crates/lpe-domain/src/tests/transport_delivery_status_serializes_as_lowercase.md)
- [outbound_envelope_recipients_include_bcc](../../../../functions/crates/lpe-domain/src/tests/outbound_envelope_recipients_include_bcc.md)
- [outbound_handoff_response_carries_structured_transport_details](../../../../functions/crates/lpe-domain/src/tests/outbound_handoff_response_carries_structured_transport_details.md)
- [smtp_submission_request_serializes_raw_message_as_base64](../../../../functions/crates/lpe-domain/src/tests/smtp_submission_request_serializes_raw_message_as_base64.md)
- [mailbox_display_name_accepts_ascii_names](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_accepts_ascii_names.md)
- [mailbox_display_name_normalizes_cafe_to_nfc_collision_key](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_normalizes_cafe_to_nfc_collision_key.md)
- [mailbox_display_name_accepts_emoji_names](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_accepts_emoji_names.md)
- [mailbox_display_name_accepts_japanese_names](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_accepts_japanese_names.md)
- [mailbox_display_name_accepts_arabic_names_without_controls](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_accepts_arabic_names_without_controls.md)
- [mailbox_display_name_accepts_hebrew_names_without_controls](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_accepts_hebrew_names_without_controls.md)
- [mailbox_display_name_rejects_control_characters](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_rejects_control_characters.md)
- [mailbox_path_rejects_empty_segments](../../../../functions/crates/lpe-domain/src/tests/mailbox_path_rejects_empty_segments.md)
- [mailbox_list_pattern_percent_matches_one_hierarchy_level](../../../../functions/crates/lpe-domain/src/tests/mailbox_list_pattern_percent_matches_one_hierarchy_level.md)
- [mailbox_list_pattern_star_matches_recursively](../../../../functions/crates/lpe-domain/src/tests/mailbox_list_pattern_star_matches_recursively.md)
- [mailbox_list_pattern_matches_unicode_after_decoding](../../../../functions/crates/lpe-domain/src/tests/mailbox_list_pattern_matches_unicode_after_decoding.md)
- [mailbox_segment_rejects_delimiter_in_segment_names](../../../../functions/crates/lpe-domain/src/tests/mailbox_segment_rejects_delimiter_in_segment_names.md)
- [mailbox_display_name_rejects_unsafe_invisible_characters](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_rejects_unsafe_invisible_characters.md)
- [mailbox_display_name_rejects_bidi_controls](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_rejects_bidi_controls.md)
- [mailbox_display_name_rejects_mixed_script_confusables](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_rejects_mixed_script_confusables.md)
- [mailbox_canonical_key_collides_for_whole_script_confusables](../../../../functions/crates/lpe-domain/src/tests/mailbox_canonical_key_collides_for_whole_script_confusables.md)
- [mailbox_display_name_rejects_reserved_name_spoofing](../../../../functions/crates/lpe-domain/src/tests/mailbox_display_name_rejects_reserved_name_spoofing.md)
- [canonical_system_display_names_are_standard_backend_names](../../../../functions/crates/lpe-domain/src/tests/canonical_system_display_names_are_standard_backend_names.md)

# Imports

- `super::{
    MailboxDisplayName, MailboxNameError, MailboxNamePolicy, MailboxPath, MailboxSegment,
    OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, SmtpSubmissionRequest,
    TransportDeliveryStatus, TransportDsnReport, TransportRecipient, TransportRetryAdvice,
    TransportRouteDecision, TransportTechnicalStatus, TransportThrottleStatus,
}`
- `uuid::Uuid`

# Member of

- [lpe-domain](../../../../packages/crates/lpe-domain.md)