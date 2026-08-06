---
type: Rust Method
title: delete_jmap_share
resource: crates/lpe-jmap/src/store.rs#L1356-L1406
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/store/shares/share_type
  - functions/crates/lpe-jmap/src/store/shares/share_uuid
  - functions/crates/lpe-jmap/src/store/shares/parse_sender_right
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`async fn delete_jmap_share(&self, share: Value, audit: AuditEntryInput) -> Result<()>`

# Calls

- [share_type](../../../../../../../functions/crates/lpe-jmap/src/store/shares/share_type.md)
- [share_uuid](../../../../../../../functions/crates/lpe-jmap/src/store/shares/share_uuid.md)
- [parse_sender_right](../../../../../../../functions/crates/lpe-jmap/src/store/shares/parse_sender_right.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)