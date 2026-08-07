---
type: Rust Function
title: upload_draft_attachment
resource: crates/lpe-admin-api/src/client_attachments.rs#L38-L78
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/client_attachments/require_attachment_mailbox_access
  - functions/crates/lpe-storage/src/attachments/Storage/message_is_visible_draft
  - functions/crates/lpe-admin-api/src/client_attachments/read_multipart_attachment
  - functions/crates/lpe-admin-api/src/client_attachments/validate_client_attachment_with_validator
  - functions/crates/lpe-admin-api/src/client_attachments/client_attachment_upload_response
---

# Signature

`pub(crate) async fn upload_draft_attachment( State(storage): State<Storage>, headers: HeaderMap, AxumPath(message_id): AxumPath<Uuid>, Query(query): Query<MessageAttachmentQuery>, mut multipart: Multipart, ) -> Result<Json<ClientAttachmentUploadResponse>, (StatusCode, String)>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [require_attachment_mailbox_access](../../../../../functions/crates/lpe-admin-api/src/client_attachments/require_attachment_mailbox_access.md)
- [message_is_visible_draft](../../../../../functions/crates/lpe-storage/src/attachments/Storage/message_is_visible_draft.md)
- [read_multipart_attachment](../../../../../functions/crates/lpe-admin-api/src/client_attachments/read_multipart_attachment.md)
- [validate_client_attachment_with_validator](../../../../../functions/crates/lpe-admin-api/src/client_attachments/validate_client_attachment_with_validator.md)
- [client_attachment_upload_response](../../../../../functions/crates/lpe-admin-api/src/client_attachments/client_attachment_upload_response.md)