---
type: Rust Method
title: parse_email_import
resource: crates/lpe-jmap/src/mail/imports.rs#L15-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/convert/resolve_creation_reference
  - functions/crates/lpe-jmap/src/upload/parse_upload_blob_id
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message
  - functions/crates/lpe-jmap/src/mail/import_validation/JmapService/validate_imported_attachments
  - functions/crates/lpe-jmap/src/convert/map_parsed_recipients
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import
---

# Signature

`pub(crate) async fn parse_email_import( &self, account: &AuthenticatedAccount, account_access: &MailboxAccountAccess, value: Value, created_ids: &HashMap<String, String>, ) -> Result<JmapImportedEmailInput>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [resolve_creation_reference](../../../../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)
- [parse_upload_blob_id](../../../../../../../functions/crates/lpe-jmap/src/upload/parse_upload_blob_id.md)
- [as_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [parse_uuid](../../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [ensure_target_mailbox_accepts_message_write](../../../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write.md)
- [parse_rfc822_message](../../../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)
- [validate_imported_attachments](../../../../../../../functions/crates/lpe-jmap/src/mail/import_validation/JmapService/validate_imported_attachments.md)
- [map_parsed_recipients](../../../../../../../functions/crates/lpe-jmap/src/convert/map_parsed_recipients.md)

# Called by

- [handle_email_import](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import.md)