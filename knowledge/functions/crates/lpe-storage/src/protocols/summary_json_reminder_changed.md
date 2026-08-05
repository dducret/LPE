---
type: Rust Function
title: summary_json_reminder_changed
resource: crates/lpe-storage/src/protocols.rs#L1392-L1397
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/jmap_string_replay_object_id
---

# Signature

`fn summary_json_reminder_changed(summary_json: &Value) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [jmap_string_replay_object_id](../../../../../functions/crates/lpe-storage/src/protocols/Storage/jmap_string_replay_object_id.md)