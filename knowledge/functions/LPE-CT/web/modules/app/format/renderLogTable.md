---
type: JavaScript Function
title: renderLogTable
resource: LPE-CT/web/modules/app/format.js#L485-L519
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/logTableState
  - functions/LPE-CT/web/modules/app/format/logGridTemplate
  - functions/LPE-CT/web/modules/app/format/sortLogItems
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/logSortIndicator
  called_by:
  - functions/LPE-CT/web/modules/app/system/renderEmailAlertLog
---

# Signature

`function renderLogTable({ tableId, container, columns, rows, emptyTitle, emptyMessage })`

# Calls

- [logTableState](../../../../../../functions/LPE-CT/web/modules/app/format/logTableState.md)
- [logGridTemplate](../../../../../../functions/LPE-CT/web/modules/app/format/logGridTemplate.md)
- [sortLogItems](../../../../../../functions/LPE-CT/web/modules/app/format/sortLogItems.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [logSortIndicator](../../../../../../functions/LPE-CT/web/modules/app/format/logSortIndicator.md)

# Called by

- [renderEmailAlertLog](../../../../../../functions/LPE-CT/web/modules/app/system/renderEmailAlertLog.md)