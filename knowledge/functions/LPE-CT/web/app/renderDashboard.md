---
type: JavaScript Function
title: renderDashboard
resource: LPE-CT/web/app.js#L101-L130
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/syncLoadingState
  - functions/LPE-CT/web/modules/app/format/healthPosture
  - functions/LPE-CT/web/modules/app/ui/setText
  - functions/LPE-CT/web/modules/app/ui/setClassName
  - functions/LPE-CT/web/modules/app/ui/renderMetric
  - functions/LPE-CT/web/modules/app/dashboard/renderOverview
  - functions/LPE-CT/web/modules/app/system/renderSystemInformation
  - functions/LPE-CT/web/modules/pages/renderPageModules
  called_by:
  - functions/LPE-CT/web/app/syncLoadingState
  - functions/LPE-CT/web/app/saveReporting
  - functions/LPE-CT/web/app/loadOps
  - functions/LPE-CT/web/app/setLocale
---

# Signature

`function renderDashboard()`

# Calls

- [syncLoadingState](../../../../functions/LPE-CT/web/app/syncLoadingState.md)
- [healthPosture](../../../../functions/LPE-CT/web/modules/app/format/healthPosture.md)
- [setText](../../../../functions/LPE-CT/web/modules/app/ui/setText.md)
- [setClassName](../../../../functions/LPE-CT/web/modules/app/ui/setClassName.md)
- [renderMetric](../../../../functions/LPE-CT/web/modules/app/ui/renderMetric.md)
- [renderOverview](../../../../functions/LPE-CT/web/modules/app/dashboard/renderOverview.md)
- [renderSystemInformation](../../../../functions/LPE-CT/web/modules/app/system/renderSystemInformation.md)
- [renderPageModules](../../../../functions/LPE-CT/web/modules/pages/renderPageModules.md)

# Called by

- [syncLoadingState](../../../../functions/LPE-CT/web/app/syncLoadingState.md)
- [saveReporting](../../../../functions/LPE-CT/web/app/saveReporting.md)
- [loadOps](../../../../functions/LPE-CT/web/app/loadOps.md)
- [setLocale](../../../../functions/LPE-CT/web/app/setLocale.md)