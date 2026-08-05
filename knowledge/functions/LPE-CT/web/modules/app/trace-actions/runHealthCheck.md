---
type: JavaScript Function
title: runHealthCheck
resource: LPE-CT/web/modules/app/trace-actions.js#L672-L678
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function runHealthCheck(opener = document.activeElement)`

# Calls

- [renderPendingDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer.md)
- [waitForNextFrame](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame.md)
- [postJson](../../../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [renderDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)