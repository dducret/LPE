---
type: Rust Function
title: submitted_recipients_from_addresses
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L646-L656
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_email
---

# Signature

`pub(in crate::mapi) fn submitted_recipients_from_addresses( addresses: &[JmapEmailAddress], ) -> Vec<SubmittedRecipientInput>`

# Called by

- [mapi_submit_from_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_email.md)