---
type: Rust Function
title: parse_reminder_id
resource: crates/lpe-jmap/src/service/helpers.rs#L75-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set
---

# Signature

`pub(super) fn parse_reminder_id(id: &str) -> Result<(String, Uuid, Option<String>)>`

# Calls

- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_reminder_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set.md)