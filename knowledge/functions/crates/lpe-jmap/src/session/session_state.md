---
type: Rust Function
title: session_state
resource: crates/lpe-jmap/src/session.rs#L291-L312
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/session/JmapService/session_document
---

# Signature

`pub(crate) fn session_state(accessible_accounts: &[MailboxAccountAccess]) -> String`

# Calls

- [opaque_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [handle_api_request_for_account](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [session_document](../../../../../functions/crates/lpe-jmap/src/session/JmapService/session_document.md)