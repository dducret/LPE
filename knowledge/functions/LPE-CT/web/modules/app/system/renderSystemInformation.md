---
type: JavaScript Function
title: renderSystemInformation
resource: LPE-CT/web/modules/app/system.js#L55-L136
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/buildLoadingRows
  - functions/LPE-CT/web/modules/app/system/getRuntimeSystem
  - functions/LPE-CT/web/modules/app/system/renderResourceGauge
  - functions/LPE-CT/web/modules/app/format/formatReportingUptime
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/system/renderSystemTable
  - functions/LPE-CT/web/modules/app/system/serviceTableRow
  - functions/LPE-CT/web/modules/app/system/diagnosticTableRow
  - functions/LPE-CT/web/modules/app/system/actionTableRow
  - functions/LPE-CT/web/modules/app/system/toolTableRow
  called_by:
  - functions/LPE-CT/web/app/renderDashboard
  - functions/LPE-CT/web/modules/app/trace-actions/runServiceAction
---

# Signature

`function renderSystemInformation()`

# Calls

- [buildLoadingRows](../../../../../../functions/LPE-CT/web/modules/app/ui/buildLoadingRows.md)
- [getRuntimeSystem](../../../../../../functions/LPE-CT/web/modules/app/system/getRuntimeSystem.md)
- [renderResourceGauge](../../../../../../functions/LPE-CT/web/modules/app/system/renderResourceGauge.md)
- [formatReportingUptime](../../../../../../functions/LPE-CT/web/modules/app/format/formatReportingUptime.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [renderSystemTable](../../../../../../functions/LPE-CT/web/modules/app/system/renderSystemTable.md)
- [serviceTableRow](../../../../../../functions/LPE-CT/web/modules/app/system/serviceTableRow.md)
- [diagnosticTableRow](../../../../../../functions/LPE-CT/web/modules/app/system/diagnosticTableRow.md)
- [actionTableRow](../../../../../../functions/LPE-CT/web/modules/app/system/actionTableRow.md)
- [toolTableRow](../../../../../../functions/LPE-CT/web/modules/app/system/toolTableRow.md)

# Called by

- [renderDashboard](../../../../../../functions/LPE-CT/web/app/renderDashboard.md)
- [runServiceAction](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runServiceAction.md)