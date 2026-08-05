---
type: Rust Method
title: accounts_for
resource: crates/lpe-storage/src/change.rs#L93-L98
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
---

# Signature

`pub fn accounts_for(&self, category: CanonicalChangeCategory) -> HashSet<Uuid>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [compute_push_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)