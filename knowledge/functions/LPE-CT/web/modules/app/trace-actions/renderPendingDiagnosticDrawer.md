---
type: JavaScript Function
title: renderPendingDiagnosticDrawer
resource: LPE-CT/web/modules/app/trace-actions.js#L640-L662
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/renderDrawerContent
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/openDiagnostic
  - functions/LPE-CT/web/modules/app/trace-actions/runHealthCheck
  - functions/LPE-CT/web/modules/app/trace-actions/connectSupport
  - functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue
  - functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool
  - functions/LPE-CT/web/modules/app/trace-actions/runSpamTest
---

# Signature

`function renderPendingDiagnosticDrawer(title, summary, opener = document.activeElement)`

# Calls

- [renderDrawerContent](../../../../../../functions/LPE-CT/web/modules/app/ui/renderDrawerContent.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)

# Called by

- [openDiagnostic](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/openDiagnostic.md)
- [runHealthCheck](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runHealthCheck.md)
- [connectSupport](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/connectSupport.md)
- [flushMailQueue](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue.md)
- [runDiagnosticTool](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool.md)
- [runSpamTest](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runSpamTest.md)