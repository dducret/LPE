---
type: Rust Function
title: resolve_creation_reference
resource: crates/lpe-jmap/src/convert.rs#L121-L133
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy
  - functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source
  - functions/crates/lpe-jmap/src/drafts/parse_email_copy
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import
  - functions/crates/lpe-jmap/src/parse/parse_submission_email_id
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
---

# Signature

`pub(crate) fn resolve_creation_reference( value: &str, created_ids: &HashMap<String, String>, ) -> String`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_blob_get](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get.md)
- [handle_blob_lookup](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup.md)
- [handle_blob_copy](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy.md)
- [resolve_upload_source](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source.md)
- [parse_email_copy](../../../../../functions/crates/lpe-jmap/src/drafts/parse_email_copy.md)
- [parse_email_import](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)
- [parse_submission_email_id](../../../../../functions/crates/lpe-jmap/src/parse/parse_submission_email_id.md)
- [handle_vacation_response_set](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)