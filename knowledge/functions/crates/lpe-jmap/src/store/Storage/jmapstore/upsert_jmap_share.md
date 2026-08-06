---
type: Rust Method
title: upsert_jmap_share
resource: crates/lpe-jmap/src/store.rs#L1289-L1354
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/store/shares/parse_sender_right
  - functions/crates/lpe-jmap/src/store/shares/project_share
---

# Signature

`async fn upsert_jmap_share( &self, input: JmapShareInput, audit: AuditEntryInput, ) -> Result<Value>`

# Calls

- [parse_sender_right](../../../../../../../functions/crates/lpe-jmap/src/store/shares/parse_sender_right.md)
- [project_share](../../../../../../../functions/crates/lpe-jmap/src/store/shares/project_share.md)