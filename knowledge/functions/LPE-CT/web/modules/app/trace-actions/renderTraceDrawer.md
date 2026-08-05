---
type: JavaScript Function
title: renderTraceDrawer
resource: LPE-CT/web/modules/app/trace-actions.js#L146-L245
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/renderDrawerContent
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/historySizeBytes
  - functions/LPE-CT/web/modules/app/format/displayMailAddress
  - functions/LPE-CT/web/modules/app/format/formatList
  - functions/LPE-CT/web/modules/app/format/traceQueueCanBeDeleted
  - functions/LPE-CT/web/modules/app/format/displayClientAddress
  - functions/LPE-CT/web/modules/app/format/formatBytes
  - functions/LPE-CT/web/modules/app/format/formatScore
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/loadTrace
---

# Signature

`function renderTraceDrawer(trace, opener = document.activeElement)`

# Calls

- [renderDrawerContent](../../../../../../functions/LPE-CT/web/modules/app/ui/renderDrawerContent.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [historySizeBytes](../../../../../../functions/LPE-CT/web/modules/app/format/historySizeBytes.md)
- [displayMailAddress](../../../../../../functions/LPE-CT/web/modules/app/format/displayMailAddress.md)
- [formatList](../../../../../../functions/LPE-CT/web/modules/app/format/formatList.md)
- [traceQueueCanBeDeleted](../../../../../../functions/LPE-CT/web/modules/app/format/traceQueueCanBeDeleted.md)
- [displayClientAddress](../../../../../../functions/LPE-CT/web/modules/app/format/displayClientAddress.md)
- [formatBytes](../../../../../../functions/LPE-CT/web/modules/app/format/formatBytes.md)
- [formatScore](../../../../../../functions/LPE-CT/web/modules/app/format/formatScore.md)

# Called by

- [loadTrace](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/loadTrace.md)