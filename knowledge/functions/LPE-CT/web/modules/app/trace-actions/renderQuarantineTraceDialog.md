---
type: JavaScript Function
title: renderQuarantineTraceDialog
resource: LPE-CT/web/modules/app/trace-actions.js#L114-L144
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/renderDrawerContent
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineDetails
  - functions/LPE-CT/web/modules/app/trace-actions/renderMessageView
  - functions/LPE-CT/web/modules/app/format/displayMailAddress
  - functions/LPE-CT/web/modules/app/format/formatList
  - functions/LPE-CT/web/modules/app/trace-actions/quarantineDialogTabButton
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/loadQuarantineTrace
  - functions/LPE-CT/web/modules/app/trace-actions/setQuarantineDialogTab
---

# Signature

`function renderQuarantineTraceDialog(trace, opener = document.activeElement)`

# Calls

- [renderDrawerContent](../../../../../../functions/LPE-CT/web/modules/app/ui/renderDrawerContent.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [renderQuarantineDetails](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineDetails.md)
- [renderMessageView](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderMessageView.md)
- [displayMailAddress](../../../../../../functions/LPE-CT/web/modules/app/format/displayMailAddress.md)
- [formatList](../../../../../../functions/LPE-CT/web/modules/app/format/formatList.md)
- [quarantineDialogTabButton](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/quarantineDialogTabButton.md)

# Called by

- [loadQuarantineTrace](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/loadQuarantineTrace.md)
- [setQuarantineDialogTab](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/setQuarantineDialogTab.md)