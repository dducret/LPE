---
type: JavaScript Function
title: renderMailQueueOutput
resource: LPE-CT/web/modules/app/trace-actions.js#L459-L490
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/statusChipClass
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/formatNumber
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticOutput
---

# Signature

`function renderMailQueueOutput(metrics, copy = getCopy())`

# Calls

- [statusChipClass](../../../../../../functions/LPE-CT/web/modules/app/format/statusChipClass.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [formatNumber](../../../../../../functions/LPE-CT/web/modules/app/format/formatNumber.md)

# Called by

- [renderDiagnosticOutput](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticOutput.md)