---
type: JavaScript Function
title: runServiceAction
resource: LPE-CT/web/modules/app/trace-actions.js#L741-L746
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/modules/app/system/renderSystemInformation
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function runServiceAction(serviceId, serviceAction)`

# Calls

- [postJson](../../../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [renderSystemInformation](../../../../../../functions/LPE-CT/web/modules/app/system/renderSystemInformation.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)