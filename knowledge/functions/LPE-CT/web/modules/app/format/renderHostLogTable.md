---
type: JavaScript Function
title: renderHostLogTable
resource: LPE-CT/web/modules/app/format.js#L572-L605
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/hostLogColumns
  - functions/LPE-CT/web/modules/app/format/logGridTemplate
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/hostLogDate
  - functions/LPE-CT/web/modules/app/format/formatBytes
  - functions/LPE-CT/web/modules/app/format/hostLogActionButton
  called_by:
  - functions/LPE-CT/web/modules/app/system/renderMailLog
  - functions/LPE-CT/web/modules/app/system/renderAudit
  - functions/LPE-CT/web/modules/app/system/renderMessageLog
---

# Signature

`function renderHostLogTable({ tableId, container, rows, emptyTitle, emptyMessage })`

# Calls

- [hostLogColumns](../../../../../../functions/LPE-CT/web/modules/app/format/hostLogColumns.md)
- [logGridTemplate](../../../../../../functions/LPE-CT/web/modules/app/format/logGridTemplate.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [hostLogDate](../../../../../../functions/LPE-CT/web/modules/app/format/hostLogDate.md)
- [formatBytes](../../../../../../functions/LPE-CT/web/modules/app/format/formatBytes.md)
- [hostLogActionButton](../../../../../../functions/LPE-CT/web/modules/app/format/hostLogActionButton.md)

# Called by

- [renderMailLog](../../../../../../functions/LPE-CT/web/modules/app/system/renderMailLog.md)
- [renderAudit](../../../../../../functions/LPE-CT/web/modules/app/system/renderAudit.md)
- [renderMessageLog](../../../../../../functions/LPE-CT/web/modules/app/system/renderMessageLog.md)