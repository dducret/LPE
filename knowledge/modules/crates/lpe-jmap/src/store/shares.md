---
type: Rust Module
title: shares
resource: crates/lpe-jmap/src/store/shares.rs#L1-L154
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-storage-collaborationresourcekind-senderdelegationright
  - external/serde-json-json-map-value
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [project_share](../../../../../functions/crates/lpe-jmap/src/store/shares/project_share.md)
- [copy_share_field](../../../../../functions/crates/lpe-jmap/src/store/shares/copy_share_field.md)
- [copy_share_field_as](../../../../../functions/crates/lpe-jmap/src/store/shares/copy_share_field_as.md)
- [share_rights](../../../../../functions/crates/lpe-jmap/src/store/shares/share_rights.md)
- [default_share_rights](../../../../../functions/crates/lpe-jmap/src/store/shares/default_share_rights.md)
- [share_type](../../../../../functions/crates/lpe-jmap/src/store/shares/share_type.md)
- [share_uuid](../../../../../functions/crates/lpe-jmap/src/store/shares/share_uuid.md)
- [parse_collaboration_kind](../../../../../functions/crates/lpe-jmap/src/store/shares/parse_collaboration_kind.md)
- [parse_sender_right](../../../../../functions/crates/lpe-jmap/src/store/shares/parse_sender_right.md)

# Imports

- `anyhow::Result`
- `lpe_storage::{CollaborationResourceKind, SenderDelegationRight}`
- `serde_json::{json, Map, Value}`
- `uuid::Uuid`

# Member of

- [lpe-jmap](../../../../../packages/crates/lpe-jmap.md)