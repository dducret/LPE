---
type: JavaScript Function
title: runDiagnosticTool
resource: LPE-CT/web/modules/app/trace-actions.js#L697-L710
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  - functions/LPE-CT/web/app/smoke/test/MockElement/focus
  - functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/diagnosticToolTitle
  - functions/LPE-CT/web/modules/app/trace-actions/diagnosticToolSummary
  - functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function runDiagnosticTool(tool, opener = document.activeElement)`

# Calls

- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)
- [focus](../../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/focus.md)
- [renderPendingDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer.md)
- [diagnosticToolTitle](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/diagnosticToolTitle.md)
- [diagnosticToolSummary](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/diagnosticToolSummary.md)
- [waitForNextFrame](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame.md)
- [postJson](../../../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [renderDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)