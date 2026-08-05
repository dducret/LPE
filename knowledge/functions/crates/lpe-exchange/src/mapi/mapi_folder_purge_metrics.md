---
type: Rust Function
title: mapi_folder_purge_metrics
resource: crates/lpe-exchange/src/mapi.rs#L168-L175
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/load
  called_by:
  - functions/crates/lpe-admin-api/src/observability/render_metrics
---

# Signature

`pub fn mapi_folder_purge_metrics() -> MapiFolderPurgeMetrics`

# Calls

- [load](../../../../../functions/LPE-CT/web/app/load.md)

# Called by

- [render_metrics](../../../../../functions/crates/lpe-admin-api/src/observability/render_metrics.md)