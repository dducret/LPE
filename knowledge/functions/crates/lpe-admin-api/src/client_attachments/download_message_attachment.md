---
type: Rust Function
title: download_message_attachment
resource: crates/lpe-admin-api/src/client_attachments.rs#L80-L103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/client_attachments/require_attachment_mailbox_access
  - functions/crates/lpe-admin-api/src/client_attachments/attachment_content_response
---

# Signature

`pub(crate) async fn download_message_attachment( State(storage): State<Storage>, headers: HeaderMap, AxumPath((message_id, attachment_id)): AxumPath<(Uuid, Uuid)>, Query(query): Query<MessageAttachmentQuery>, ) -> Result<Response, (StatusCode, String)>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [require_attachment_mailbox_access](../../../../../functions/crates/lpe-admin-api/src/client_attachments/require_attachment_mailbox_access.md)
- [attachment_content_response](../../../../../functions/crates/lpe-admin-api/src/client_attachments/attachment_content_response.md)