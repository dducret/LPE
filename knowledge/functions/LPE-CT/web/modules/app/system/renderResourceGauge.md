---
type: JavaScript Function
title: renderResourceGauge
resource: LPE-CT/web/modules/app/system.js#L14-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/formatPercent
  - functions/LPE-CT/web/modules/app/system/formatGigabytes
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  called_by:
  - functions/LPE-CT/web/modules/app/system/renderSystemInformation
---

# Signature

`function renderResourceGauge(percentValue, totalBytes)`

# Calls

- [formatPercent](../../../../../../functions/LPE-CT/web/modules/app/format/formatPercent.md)
- [formatGigabytes](../../../../../../functions/LPE-CT/web/modules/app/system/formatGigabytes.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)

# Called by

- [renderSystemInformation](../../../../../../functions/LPE-CT/web/modules/app/system/renderSystemInformation.md)