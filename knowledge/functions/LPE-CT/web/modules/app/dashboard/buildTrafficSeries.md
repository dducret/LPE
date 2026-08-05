---
type: JavaScript Function
title: buildTrafficSeries
resource: LPE-CT/web/modules/app/dashboard.js#L165-L205
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/formatShortDate
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/web/modules/app/dashboard/classifyTrafficItem
  called_by:
  - functions/LPE-CT/web/modules/app/dashboard/renderOverview
---

# Signature

`function buildTrafficSeries(records)`

# Calls

- [formatShortDate](../../../../../../functions/LPE-CT/web/modules/app/format/formatShortDate.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [classifyTrafficItem](../../../../../../functions/LPE-CT/web/modules/app/dashboard/classifyTrafficItem.md)

# Called by

- [renderOverview](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderOverview.md)