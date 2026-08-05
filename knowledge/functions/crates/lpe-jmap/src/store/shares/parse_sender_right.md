---
type: Rust Function
title: parse_sender_right
resource: crates/lpe-jmap/src/store/shares.rs#L148-L154
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_jmap_share
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_share
---

# Signature

`pub(super) fn parse_sender_right(value: Option<&str>) -> Result<SenderDelegationRight>`

# Called by

- [upsert_jmap_share](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_jmap_share.md)
- [delete_jmap_share](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_share.md)