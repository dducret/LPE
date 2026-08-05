---
type: Rust Module
title: util
resource: crates/lpe-admin-api/src/util.rs#L1-L56
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-statuscode
  - external/lpe-storage-admindashboard-authenticatedadmin-collaborationresourcekind-senderdelegationright
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [parse_collaboration_kind](../../../../functions/crates/lpe-admin-api/src/util/parse_collaboration_kind.md)
- [parse_sender_delegation_right](../../../../functions/crates/lpe-admin-api/src/util/parse_sender_delegation_right.md)
- [ensure_admin_can_manage_email](../../../../functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email.md)
- [mailbox_account_email](../../../../functions/crates/lpe-admin-api/src/util/mailbox_account_email.md)

# Imports

- `axum::http::StatusCode`
- `lpe_storage::{
    AdminDashboard, AuthenticatedAdmin, CollaborationResourceKind, SenderDelegationRight,
}`
- `uuid::Uuid`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)