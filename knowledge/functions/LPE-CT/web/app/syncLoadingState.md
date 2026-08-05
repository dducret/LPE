---
type: JavaScript Function
title: syncLoadingState
resource: LPE-CT/web/app.js#L13-L34
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/setText
  - functions/LPE-CT/web/modules/app/system/renderHostClock
  - functions/LPE-CT/web/modules/app/dashboard/renderOverview
  - functions/LPE-CT/web/modules/app/ui/setListLoading
  - functions/LPE-CT/web/app/renderDashboard
  called_by:
  - functions/LPE-CT/web/app/renderDashboard
  - functions/LPE-CT/web/app/load
  - functions/LPE-CT/web/app/setLocale
---

# Signature

`function syncLoadingState()`

# Calls

- [setText](../../../../functions/LPE-CT/web/modules/app/ui/setText.md)
- [renderHostClock](../../../../functions/LPE-CT/web/modules/app/system/renderHostClock.md)
- [renderOverview](../../../../functions/LPE-CT/web/modules/app/dashboard/renderOverview.md)
- [setListLoading](../../../../functions/LPE-CT/web/modules/app/ui/setListLoading.md)
- [renderDashboard](../../../../functions/LPE-CT/web/app/renderDashboard.md)

# Called by

- [renderDashboard](../../../../functions/LPE-CT/web/app/renderDashboard.md)
- [load](../../../../functions/LPE-CT/web/app/load.md)
- [setLocale](../../../../functions/LPE-CT/web/app/setLocale.md)