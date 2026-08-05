---
type: JavaScript Function
title: flushMailQueue
resource: LPE-CT/web/modules/app/trace-actions.js#L688-L695
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer
  - functions/LPE-CT/web/app/loadOps
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function flushMailQueue(opener = document.activeElement)`

# Calls

- [renderPendingDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer.md)
- [waitForNextFrame](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame.md)
- [postJson](../../../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [renderDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer.md)
- [loadOps](../../../../../../functions/LPE-CT/web/app/loadOps.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)