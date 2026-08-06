---
type: Rust Function
title: is_mapi_only_change
resource: crates/lpe-storage/src/protocols.rs#L1313-L1318
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_mail_object_changes
---

# Signature

`fn is_mapi_only_change(summary_json: &Value) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [replay_jmap_mail_object_changes](../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_mail_object_changes.md)