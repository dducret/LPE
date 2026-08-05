---
type: Rust Function
title: map_parsed_recipients
resource: crates/lpe-jmap/src/convert.rs#L220-L230
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import
---

# Signature

`pub(crate) fn map_parsed_recipients( recipients: Vec<ParsedMailAddress>, ) -> Vec<SubmittedRecipientInput>`

# Called by

- [parse_email_import](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)