---
type: Rust Function
title: snapshot_fingerprints
resource: crates/lpe-activesync/src/snapshot.rs#L536-L556
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/diff_snapshots
---

# Signature

`fn snapshot_fingerprints(snapshot: Option<&Value>) -> HashMap<String, String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [diff_snapshots](../../../../../functions/crates/lpe-activesync/src/snapshot/diff_snapshots.md)