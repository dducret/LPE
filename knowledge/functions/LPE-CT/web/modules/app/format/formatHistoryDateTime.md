---
type: JavaScript Function
title: formatHistoryDateTime
resource: LPE-CT/web/modules/app/format.js#L106-L117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/parseHistoryTimestamp
  called_by:
  - functions/LPE-CT/web/modules/app/format/historyColumns
  - functions/LPE-CT/web/modules/app/format/quarantineColumns
  - functions/LPE-CT/web/modules/app/format/auditColumns
  - functions/LPE-CT/web/modules/app/format/messageLogColumns
  - functions/LPE-CT/web/modules/app/format/emailAlertLogColumns
  - functions/LPE-CT/web/modules/app/format/hostLogDate
  - functions/LPE-CT/web/modules/app/trace-actions/renderMessageView
---

# Signature

`function formatHistoryDateTime(value)`

# Calls

- [parseHistoryTimestamp](../../../../../../functions/LPE-CT/web/modules/app/format/parseHistoryTimestamp.md)

# Called by

- [historyColumns](../../../../../../functions/LPE-CT/web/modules/app/format/historyColumns.md)
- [quarantineColumns](../../../../../../functions/LPE-CT/web/modules/app/format/quarantineColumns.md)
- [auditColumns](../../../../../../functions/LPE-CT/web/modules/app/format/auditColumns.md)
- [messageLogColumns](../../../../../../functions/LPE-CT/web/modules/app/format/messageLogColumns.md)
- [emailAlertLogColumns](../../../../../../functions/LPE-CT/web/modules/app/format/emailAlertLogColumns.md)
- [hostLogDate](../../../../../../functions/LPE-CT/web/modules/app/format/hostLogDate.md)
- [renderMessageView](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderMessageView.md)