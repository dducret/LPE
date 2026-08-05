---
type: Rust Method
title: session_document
resource: crates/lpe-jmap/src/session.rs#L22-L78
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/session_capabilities
  - functions/crates/lpe-jmap/src/session/normalize_public_base_url
  - functions/crates/lpe-jmap/src/session/mailbox_account_is_read_only
  - functions/crates/lpe-jmap/src/session/session_account_capabilities
  - functions/crates/lpe-jmap/src/session/session_state
  called_by:
  - functions/crates/lpe-jmap/src/service/session_handler
  - functions/crates/lpe-jmap/src/tests/session_uses_existing_account_authentication
  - functions/crates/lpe-jmap/src/tests/session_urls_respect_forwarded_jmap_prefix
  - functions/crates/lpe-jmap/src/tests/session_state_tracks_accessible_mailbox_projection
  - functions/crates/lpe-jmap/src/tests/session_and_identity_include_accessible_shared_mailbox_accounts
  - functions/crates/lpe-jmap/src/tests/session_omits_submission_for_shared_mailbox_without_sender_grant
  - functions/crates/lpe-jmap/src/tests/session_omits_submission_for_read_only_shared_mailbox_with_sender_grant
  - functions/crates/lpe-jmap/src/tests/session_exposes_contacts_and_calendars_capabilities
---

# Signature

`pub async fn session_document( &self, authorization: Option<&str>, websocket_url: Option<&str>, public_base_url: Option<&str>, ) -> Result<SessionDocument>`

# Calls

- [session_capabilities](../../../../../../functions/crates/lpe-jmap/src/session/session_capabilities.md)
- [normalize_public_base_url](../../../../../../functions/crates/lpe-jmap/src/session/normalize_public_base_url.md)
- [mailbox_account_is_read_only](../../../../../../functions/crates/lpe-jmap/src/session/mailbox_account_is_read_only.md)
- [session_account_capabilities](../../../../../../functions/crates/lpe-jmap/src/session/session_account_capabilities.md)
- [session_state](../../../../../../functions/crates/lpe-jmap/src/session/session_state.md)

# Called by

- [session_handler](../../../../../../functions/crates/lpe-jmap/src/service/session_handler.md)
- [session_uses_existing_account_authentication](../../../../../../functions/crates/lpe-jmap/src/tests/session_uses_existing_account_authentication.md)
- [session_urls_respect_forwarded_jmap_prefix](../../../../../../functions/crates/lpe-jmap/src/tests/session_urls_respect_forwarded_jmap_prefix.md)
- [session_state_tracks_accessible_mailbox_projection](../../../../../../functions/crates/lpe-jmap/src/tests/session_state_tracks_accessible_mailbox_projection.md)
- [session_and_identity_include_accessible_shared_mailbox_accounts](../../../../../../functions/crates/lpe-jmap/src/tests/session_and_identity_include_accessible_shared_mailbox_accounts.md)
- [session_omits_submission_for_shared_mailbox_without_sender_grant](../../../../../../functions/crates/lpe-jmap/src/tests/session_omits_submission_for_shared_mailbox_without_sender_grant.md)
- [session_omits_submission_for_read_only_shared_mailbox_with_sender_grant](../../../../../../functions/crates/lpe-jmap/src/tests/session_omits_submission_for_read_only_shared_mailbox_with_sender_grant.md)
- [session_exposes_contacts_and_calendars_capabilities](../../../../../../functions/crates/lpe-jmap/src/tests/session_exposes_contacts_and_calendars_capabilities.md)