---
type: Rust Function
title: message_recipients
resource: crates/lpe-exchange/src/mapi/tables/recipients.rs#L9-L43
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/message_can_expose_bcc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_open_message_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/pending_recipients_from_email
  - functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response
---

# Signature

`pub(in crate::mapi) fn message_recipients(email: &JmapEmail) -> Vec<MapiRecipient<'_>>`

# Calls

- [message_can_expose_bcc](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/message_can_expose_bcc.md)

# Called by

- [log_open_message_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_open_message_debug.md)
- [pending_recipients_from_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/pending_recipients_from_email.md)
- [rop_read_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)
- [restriction_matches_email_with_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [rop_open_message_response_with_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients.md)
- [rop_reload_cached_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response.md)