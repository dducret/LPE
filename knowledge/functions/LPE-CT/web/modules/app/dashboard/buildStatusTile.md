---
type: JavaScript Function
title: buildStatusTile
resource: LPE-CT/web/modules/app/dashboard.js#L19-L27
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/statusChipClass
  called_by:
  - functions/LPE-CT/web/modules/app/dashboard/renderOverview
---

# Signature

`function buildStatusTile(title, stateLabel, tone = "muted", detail = "")`

# Calls

- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [statusChipClass](../../../../../../functions/LPE-CT/web/modules/app/format/statusChipClass.md)

# Called by

- [renderOverview](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderOverview.md)