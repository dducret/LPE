---
type: JavaScript Function
title: downloadHostLog
resource: LPE-CT/web/modules/app/trace-actions.js#L273-L283
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/api/fetchBlob
  - functions/LPE-CT/web/app/smoke/test/MockElement/appendChild
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function downloadHostLog(category, logId)`

# Calls

- [fetchBlob](../../../../../../functions/LPE-CT/web/modules/app/api/fetchBlob.md)
- [appendChild](../../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/appendChild.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)