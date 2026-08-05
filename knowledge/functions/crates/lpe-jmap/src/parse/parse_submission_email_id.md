---
type: Rust Function
title: parse_submission_email_id
resource: crates/lpe-jmap/src/parse.rs#L19-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/convert/resolve_creation_reference
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set
---

# Signature

`pub(crate) fn parse_submission_email_id( value: &Value, created_ids: &HashMap<String, String>, ) -> Result<Option<String>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [resolve_creation_reference](../../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)

# Called by

- [handle_email_submission_set](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set.md)