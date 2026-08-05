---
type: JavaScript Function
title: renderHostClock
resource: LPE-CT/web/modules/app/system.js#L805-L809
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/formatDateTime
  - functions/LPE-CT/web/modules/app/system/getHostClockDate
  - functions/LPE-CT/web/modules/app/ui/setText
  called_by:
  - functions/LPE-CT/web/app/syncLoadingState
  - functions/LPE-CT/web/modules/app/dashboard/renderOverview
---

# Signature

`function renderHostClock()`

# Calls

- [formatDateTime](../../../../../../functions/LPE-CT/web/modules/app/format/formatDateTime.md)
- [getHostClockDate](../../../../../../functions/LPE-CT/web/modules/app/system/getHostClockDate.md)
- [setText](../../../../../../functions/LPE-CT/web/modules/app/ui/setText.md)

# Called by

- [syncLoadingState](../../../../../../functions/LPE-CT/web/app/syncLoadingState.md)
- [renderOverview](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderOverview.md)