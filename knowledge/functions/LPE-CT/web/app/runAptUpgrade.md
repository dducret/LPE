---
type: JavaScript Function
title: runAptUpgrade
resource: LPE-CT/web/app.js#L155-L162
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/app/loadOps
  - functions/LPE-CT/web/modules/app/system/renderPlatform
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function runAptUpgrade()`

# Calls

- [showFeedback](../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)
- [postJson](../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [loadOps](../../../../functions/LPE-CT/web/app/loadOps.md)
- [renderPlatform](../../../../functions/LPE-CT/web/modules/app/system/renderPlatform.md)

# Called by

- [getActionHandlers](../../../../functions/LPE-CT/web/app/getActionHandlers.md)