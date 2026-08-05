---
type: JavaScript Function
title: loadTrace
resource: LPE-CT/web/modules/app/trace-actions.js#L293-L305
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/renderDrawerContent
  - functions/LPE-CT/web/modules/app/ui/buildLoadingRows
  - functions/LPE-CT/web/modules/app/trace-actions/renderTraceDrawer
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
  - functions/LPE-CT/web/modules/app/trace-actions/triggerTraceAction
---

# Signature

`async function loadTrace(traceId, opener = document.activeElement)`

# Calls

- [renderDrawerContent](../../../../../../functions/LPE-CT/web/modules/app/ui/renderDrawerContent.md)
- [buildLoadingRows](../../../../../../functions/LPE-CT/web/modules/app/ui/buildLoadingRows.md)
- [renderTraceDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderTraceDrawer.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)
- [triggerTraceAction](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/triggerTraceAction.md)