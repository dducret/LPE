---
type: Rust Function
title: inspect_queue
resource: LPE-CT/src/smtp/queue_store.rs#L35-L65
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/queue_metrics
---

# Signature

`pub(in crate::smtp) fn inspect_queue(spool_dir: &Path, queue: &str) -> Result<QueueInspection>`

# Called by

- [queue_metrics](../../../../../functions/LPE-CT/src/smtp/queue_metrics.md)