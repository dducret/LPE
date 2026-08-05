---
type: JavaScript Function
title: renderDiagnosticDrawer
resource: LPE-CT/web/modules/app/trace-actions.js#L413-L432
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/renderDrawerContent
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/statusChipClass
  - functions/LPE-CT/web/modules/app/format/humanizeStatus
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticOutput
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/openDiagnostic
  - functions/LPE-CT/web/modules/app/trace-actions/runHealthCheck
  - functions/LPE-CT/web/modules/app/trace-actions/connectSupport
  - functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue
  - functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool
  - functions/LPE-CT/web/modules/app/trace-actions/runSpamTest
---

# Signature

`function renderDiagnosticDrawer(report, opener = document.activeElement)`

# Calls

- [renderDrawerContent](../../../../../../functions/LPE-CT/web/modules/app/ui/renderDrawerContent.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [statusChipClass](../../../../../../functions/LPE-CT/web/modules/app/format/statusChipClass.md)
- [humanizeStatus](../../../../../../functions/LPE-CT/web/modules/app/format/humanizeStatus.md)
- [renderDiagnosticOutput](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticOutput.md)

# Called by

- [openDiagnostic](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/openDiagnostic.md)
- [runHealthCheck](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runHealthCheck.md)
- [connectSupport](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/connectSupport.md)
- [flushMailQueue](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue.md)
- [runDiagnosticTool](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool.md)
- [runSpamTest](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runSpamTest.md)