---
type: Rust Function
title: mail_flow
resource: crates/lpe-admin-api/src/console.rs#L748-L758
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/admin/Storage/fetch_mail_flow_entries
---

# Signature

`pub(crate) async fn mail_flow( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<MailFlowResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [fetch_mail_flow_entries](../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_mail_flow_entries.md)