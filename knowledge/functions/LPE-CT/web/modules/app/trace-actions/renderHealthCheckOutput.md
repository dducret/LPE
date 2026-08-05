---
type: JavaScript Function
title: renderHealthCheckOutput
resource: LPE-CT/web/modules/app/trace-actions.js#L511-L576
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/statusChipClass
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/humanizeStatus
  - functions/LPE-CT/web/modules/app/format/formatNumber
  - functions/LPE-CT/web/modules/app/trace-actions/healthCheckSummaryValue
  - functions/LPE-CT/web/modules/app/trace-actions/healthCheckMarkerClass
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticOutput
---

# Signature

`function renderHealthCheckOutput(readiness, copy = getCopy())`

# Calls

- [statusChipClass](../../../../../../functions/LPE-CT/web/modules/app/format/statusChipClass.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [humanizeStatus](../../../../../../functions/LPE-CT/web/modules/app/format/humanizeStatus.md)
- [formatNumber](../../../../../../functions/LPE-CT/web/modules/app/format/formatNumber.md)
- [healthCheckSummaryValue](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/healthCheckSummaryValue.md)
- [healthCheckMarkerClass](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/healthCheckMarkerClass.md)

# Called by

- [renderDiagnosticOutput](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticOutput.md)