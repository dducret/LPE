---
type: JavaScript Function
title: formatResourceUsage
resource: LPE-CT/web/modules/app/dashboard.js#L207-L220
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/formatPercent
  - functions/LPE-CT/web/modules/app/format/formatBytes
  called_by:
  - functions/LPE-CT/web/modules/app/dashboard/renderSystemOverview
---

# Signature

`function formatResourceUsage(usedPercent, total)`

# Calls

- [formatPercent](../../../../../../functions/LPE-CT/web/modules/app/format/formatPercent.md)
- [formatBytes](../../../../../../functions/LPE-CT/web/modules/app/format/formatBytes.md)

# Called by

- [renderSystemOverview](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderSystemOverview.md)