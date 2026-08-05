---
type: Rust Function
title: parse_share_input
resource: crates/lpe-jmap/src/service/helpers.rs#L87-L137
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_share_set
---

# Signature

`pub(super) fn parse_share_input(owner_account_id: Uuid, value: &Value) -> Result<JmapShareInput>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_share_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_set.md)