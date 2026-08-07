---
type: Rust Method
title: fetch_client_workspace
resource: crates/lpe-storage/src/workspace.rs#L205-L371
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks
  - functions/crates/lpe-storage/src/attachments/attachment_kind
  - functions/crates/lpe-storage/src/workspace/format_size
  - functions/crates/lpe-storage/src/workspace/client_folder
  - functions/crates/lpe-storage/src/workspace/client_message_tags
  - functions/crates/lpe-storage/src/workspace/body_paragraphs
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/client_workspace
---

# Signature

`pub async fn fetch_client_workspace(&self, account_id: Uuid) -> Result<ClientWorkspace>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [fetch_client_tasks](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks.md)
- [attachment_kind](../../../../../../functions/crates/lpe-storage/src/attachments/attachment_kind.md)
- [format_size](../../../../../../functions/crates/lpe-storage/src/workspace/format_size.md)
- [client_folder](../../../../../../functions/crates/lpe-storage/src/workspace/client_folder.md)
- [client_message_tags](../../../../../../functions/crates/lpe-storage/src/workspace/client_message_tags.md)
- [body_paragraphs](../../../../../../functions/crates/lpe-storage/src/workspace/body_paragraphs.md)

# Called by

- [client_workspace](../../../../../../functions/crates/lpe-admin-api/src/workspace/client_workspace.md)