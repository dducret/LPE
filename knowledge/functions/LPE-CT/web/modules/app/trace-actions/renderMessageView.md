---
type: JavaScript Function
title: renderMessageView
resource: LPE-CT/web/modules/app/trace-actions.js#L79-L112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/traceHeadersText
  - functions/LPE-CT/web/modules/app/format/traceAttachmentItems
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/formatBytes
  - functions/LPE-CT/web/modules/app/format/traceHeaderValue
  - functions/LPE-CT/web/modules/app/format/displayMailAddress
  - functions/LPE-CT/web/modules/app/format/formatList
  - functions/LPE-CT/web/modules/app/format/formatHistoryDateTime
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineTraceDialog
---

# Signature

`function renderMessageView(current)`

# Calls

- [traceHeadersText](../../../../../../functions/LPE-CT/web/modules/app/format/traceHeadersText.md)
- [traceAttachmentItems](../../../../../../functions/LPE-CT/web/modules/app/format/traceAttachmentItems.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [formatBytes](../../../../../../functions/LPE-CT/web/modules/app/format/formatBytes.md)
- [traceHeaderValue](../../../../../../functions/LPE-CT/web/modules/app/format/traceHeaderValue.md)
- [displayMailAddress](../../../../../../functions/LPE-CT/web/modules/app/format/displayMailAddress.md)
- [formatList](../../../../../../functions/LPE-CT/web/modules/app/format/formatList.md)
- [formatHistoryDateTime](../../../../../../functions/LPE-CT/web/modules/app/format/formatHistoryDateTime.md)

# Called by

- [renderQuarantineTraceDialog](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineTraceDialog.md)