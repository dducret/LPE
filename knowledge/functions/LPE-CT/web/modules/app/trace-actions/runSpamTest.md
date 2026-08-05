---
type: JavaScript Function
title: runSpamTest
resource: LPE-CT/web/modules/app/trace-actions.js#L712-L729
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  - functions/LPE-CT/web/app/smoke/test/MockElement/focus
  - functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame
  - functions/LPE-CT/web/modules/app/trace-actions/fileToBase64
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function runSpamTest(opener = document.activeElement)`

# Calls

- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)
- [focus](../../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/focus.md)
- [renderPendingDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer.md)
- [waitForNextFrame](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame.md)
- [fileToBase64](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/fileToBase64.md)
- [postJson](../../../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [renderDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)