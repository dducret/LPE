---
type: Rust Method
title: message_is_visible_draft
resource: crates/lpe-storage/src/attachments.rs#L50-L75
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  called_by:
  - functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment
---

# Signature

`pub async fn message_is_visible_draft( &self, account_id: Uuid, message_id: Uuid, ) -> Result<bool>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)

# Called by

- [upload_draft_attachment](../../../../../../functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment.md)