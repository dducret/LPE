---
type: Rust Method
title: canonical_status
resource: crates/lpe-exchange/src/ews_types.rs#L381-L388
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/tasks/ews_task_status_to_canonical
---

# Signature

`pub(crate) fn canonical_status(self) -> &'static str`

# Called by

- [ews_task_status_to_canonical](../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/ews_task_status_to_canonical.md)