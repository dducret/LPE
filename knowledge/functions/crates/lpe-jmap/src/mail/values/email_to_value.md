---
type: Rust Function
title: email_to_value
resource: crates/lpe-jmap/src/mail/values.rs#L270-L451
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  - functions/crates/lpe-jmap/src/upload/blob_id_for_message
  - functions/crates/lpe-jmap/src/mail/values/email_keywords
  - functions/crates/lpe-jmap/src/mail/values/email_followup_value
  - functions/crates/lpe-jmap/src/convert/address_value
  - functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/should_fetch_text_value
  - functions/crates/lpe-jmap/src/mail/values/body_value
  - functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/should_fetch_html_value
---

# Signature

`pub(crate) fn email_to_value( email: &JmapEmail, properties: &HashSet<String>, body_options: &EmailBodyOptions, include_owner_bcc: bool, ) -> Value`

# Calls

- [insert_if](../../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)
- [blob_id_for_message](../../../../../../functions/crates/lpe-jmap/src/upload/blob_id_for_message.md)
- [email_keywords](../../../../../../functions/crates/lpe-jmap/src/mail/values/email_keywords.md)
- [email_followup_value](../../../../../../functions/crates/lpe-jmap/src/mail/values/email_followup_value.md)
- [address_value](../../../../../../functions/crates/lpe-jmap/src/convert/address_value.md)
- [should_fetch_text_value](../../../../../../functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/should_fetch_text_value.md)
- [body_value](../../../../../../functions/crates/lpe-jmap/src/mail/values/body_value.md)
- [should_fetch_html_value](../../../../../../functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/should_fetch_html_value.md)