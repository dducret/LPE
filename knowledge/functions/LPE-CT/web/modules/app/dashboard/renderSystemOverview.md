---
type: JavaScript Function
title: renderSystemOverview
resource: LPE-CT/web/modules/app/dashboard.js#L246-L283
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/system/getRuntimeSystem
  - functions/LPE-CT/web/modules/app/format/formatDateTime
  - functions/LPE-CT/web/modules/app/system/getHostClockDate
  - functions/LPE-CT/web/modules/app/format/formatUptime
  - functions/LPE-CT/web/modules/app/format/formatPercent
  - functions/LPE-CT/web/modules/app/format/formatNumber
  - functions/LPE-CT/web/modules/app/dashboard/formatResourceUsage
  - functions/LPE-CT/web/modules/app/dashboard/renderDashboardDetailTable
  called_by:
  - functions/LPE-CT/web/modules/app/dashboard/renderOverview
---

# Signature

`function renderSystemOverview(dashboard, copy)`

# Calls

- [getRuntimeSystem](../../../../../../functions/LPE-CT/web/modules/app/system/getRuntimeSystem.md)
- [formatDateTime](../../../../../../functions/LPE-CT/web/modules/app/format/formatDateTime.md)
- [getHostClockDate](../../../../../../functions/LPE-CT/web/modules/app/system/getHostClockDate.md)
- [formatUptime](../../../../../../functions/LPE-CT/web/modules/app/format/formatUptime.md)
- [formatPercent](../../../../../../functions/LPE-CT/web/modules/app/format/formatPercent.md)
- [formatNumber](../../../../../../functions/LPE-CT/web/modules/app/format/formatNumber.md)
- [formatResourceUsage](../../../../../../functions/LPE-CT/web/modules/app/dashboard/formatResourceUsage.md)
- [renderDashboardDetailTable](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderDashboardDetailTable.md)

# Called by

- [renderOverview](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderOverview.md)