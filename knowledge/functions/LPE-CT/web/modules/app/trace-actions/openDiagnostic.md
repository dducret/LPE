---
type: JavaScript Function
title: openDiagnostic
resource: LPE-CT/web/modules/app/trace-actions.js#L664-L670
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/diagnosticTitle
  - functions/LPE-CT/web/modules/app/trace-actions/diagnosticSummary
  - functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function openDiagnostic(kind, opener = document.activeElement)`

# Calls

- [renderPendingDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer.md)
- [diagnosticTitle](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/diagnosticTitle.md)
- [diagnosticSummary](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/diagnosticSummary.md)
- [waitForNextFrame](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/waitForNextFrame.md)
- [renderDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)