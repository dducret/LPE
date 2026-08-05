---
type: Rust Function
title: apply_state_changes
resource: crates/lpe-jmap/src/state.rs#L294-L327
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/state/finish_changes_response
---

# Signature

`fn apply_state_changes( previous_entries: Vec<StateEntry>, current_map: &HashMap<String, String>, created: &[String], updated: &[String], destroyed: &[String], ) -> Vec<StateEntry>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [finish_changes_response](../../../../../functions/crates/lpe-jmap/src/state/finish_changes_response.md)