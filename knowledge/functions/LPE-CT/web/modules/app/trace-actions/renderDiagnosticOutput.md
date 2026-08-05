---
type: JavaScript Function
title: renderDiagnosticOutput
resource: LPE-CT/web/modules/app/trace-actions.js#L434-L448
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/trace-actions/parseJsonObject
  - functions/LPE-CT/web/modules/app/trace-actions/renderMailQueueOutput
  - functions/LPE-CT/web/modules/app/trace-actions/renderHealthCheckOutput
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer
---

# Signature

`function renderDiagnosticOutput(report, copy = getCopy())`

# Calls

- [parseJsonObject](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/parseJsonObject.md)
- [renderMailQueueOutput](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderMailQueueOutput.md)
- [renderHealthCheckOutput](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderHealthCheckOutput.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)

# Called by

- [renderDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer.md)