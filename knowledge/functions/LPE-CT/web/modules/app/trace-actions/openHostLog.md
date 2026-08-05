---
type: JavaScript Function
title: openHostLog
resource: LPE-CT/web/modules/app/trace-actions.js#L248-L271
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/renderDrawerContent
  - functions/LPE-CT/web/modules/app/ui/buildLoadingRows
  - functions/LPE-CT/web/modules/app/format/formatBytes
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function openHostLog(category, logId, opener = document.activeElement)`

# Calls

- [renderDrawerContent](../../../../../../functions/LPE-CT/web/modules/app/ui/renderDrawerContent.md)
- [buildLoadingRows](../../../../../../functions/LPE-CT/web/modules/app/ui/buildLoadingRows.md)
- [formatBytes](../../../../../../functions/LPE-CT/web/modules/app/format/formatBytes.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)