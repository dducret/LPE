---
type: JavaScript Function
title: renderDrawerContent
resource: LPE-CT/web/modules/app/ui.js#L97-L111
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/toggle
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/LPE-CT/web/app/smoke/test/MockElement/querySelector
  - functions/LPE-CT/web/app/smoke/test/MockElement/focus
  called_by:
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineTraceDialog
  - functions/LPE-CT/web/modules/app/trace-actions/renderTraceDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/openHostLog
  - functions/LPE-CT/web/modules/app/trace-actions/loadTrace
  - functions/LPE-CT/web/modules/app/trace-actions/loadQuarantineTrace
  - functions/LPE-CT/web/modules/app/trace-actions/openDigestReport
  - functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer
---

# Signature

`function renderDrawerContent(title, summary, content, opener = document.activeElement, onClose = null, variant = "")`

# Calls

- [toggle](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/toggle.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [add](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [querySelector](../../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/querySelector.md)
- [focus](../../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/focus.md)

# Called by

- [renderDrawerForm](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [renderQuarantineTraceDialog](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineTraceDialog.md)
- [renderTraceDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderTraceDrawer.md)
- [openHostLog](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/openHostLog.md)
- [loadTrace](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/loadTrace.md)
- [loadQuarantineTrace](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/loadQuarantineTrace.md)
- [openDigestReport](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/openDigestReport.md)
- [renderDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderDiagnosticDrawer.md)
- [renderPendingDiagnosticDrawer](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderPendingDiagnosticDrawer.md)