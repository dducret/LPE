---
type: Rust Module
title: grants
resource: crates/lpe-storage/src/collaboration/grants.rs#L1-L820
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-normalize-email-auditentryinput-canonicalchangecategory-collaborationgrantrow-storage-default-task-list-role
  - external/super-types-map-collaboration-grant-validate-collaboration-rights-collaborationgrant-collaborationgrantinput-collaborationresourcekind
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [upsert_collaboration_grant](../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [delete_collaboration_grant](../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_collaboration_grant.md)
- [delete_calendar_collection_grant](../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_calendar_collection_grant.md)
- [set_calendar_collection_grant](../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant.md)
- [fetch_collaboration_grant](../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_collaboration_grant.md)
- [fetch_outgoing_collaboration_grants](../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    normalize_email, AuditEntryInput, CanonicalChangeCategory, CollaborationGrantRow, Storage,
    DEFAULT_TASK_LIST_ROLE,
}`
- `super::types::{
    map_collaboration_grant, validate_collaboration_rights, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind,
}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)