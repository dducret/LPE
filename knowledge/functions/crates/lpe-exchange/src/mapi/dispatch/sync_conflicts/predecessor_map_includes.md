---
type: Rust Function
title: predecessor_map_includes
resource: crates/lpe-exchange/src/mapi/dispatch/sync_conflicts.rs#L68-L84
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/sync_import_version_relation
---

# Signature

`fn predecessor_map_includes( candidate: &BTreeMap<[u8; 16], Vec<u8>>, predecessor: &BTreeMap<[u8; 16], Vec<u8>>, ) -> Result<bool>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [sync_import_version_relation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/sync_import_version_relation.md)