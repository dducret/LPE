---
type: JavaScript Function
title: activatePageView
resource: LPE-CT/web/modules/pages/index.js#L31-L45
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/pages/resolvePageId
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/web/app/smoke/test/MockClassList/toggle
  - functions/LPE-CT/web/app/smoke/test/MockElement/setAttribute
  called_by:
  - functions/LPE-CT/web/app/setActivePage
---

# Signature

`function activatePageView(pageId, { pageViews, navButtons })`

# Calls

- [resolvePageId](../../../../../functions/LPE-CT/web/modules/pages/resolvePageId.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [toggle](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/toggle.md)
- [setAttribute](../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/setAttribute.md)

# Called by

- [setActivePage](../../../../../functions/LPE-CT/web/app/setActivePage.md)