---
type: JavaScript Function
title: openPlatformDrawer
resource: LPE-CT/web/app.js#L491-L522
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/getPlatformDrawerConfigs
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/api/putJson
  - functions/LPE-CT/src/dkim_signing/payload
  - functions/LPE-CT/web/app/loadOps
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`function openPlatformDrawer(target, opener = document.activeElement)`

# Calls

- [getPlatformDrawerConfigs](../../../../functions/LPE-CT/web/app/getPlatformDrawerConfigs.md)
- [renderDrawerForm](../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [putJson](../../../../functions/LPE-CT/web/modules/app/api/putJson.md)
- [payload](../../../../functions/LPE-CT/src/dkim_signing/payload.md)
- [loadOps](../../../../functions/LPE-CT/web/app/loadOps.md)
- [closeDrawer](../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)
- [showFeedback](../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../functions/LPE-CT/web/app/getActionHandlers.md)