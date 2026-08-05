---
type: JavaScript Function
title: renderOverview
resource: LPE-CT/web/modules/app/dashboard.js#L329-L481
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/buildLoadingRows
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/format/getOperatorEmail
  - functions/LPE-CT/web/modules/app/format/getDigestSettings
  - functions/LPE-CT/web/modules/app/format/getTrafficRecords
  - functions/LPE-CT/web/modules/app/format/healthPosture
  - functions/LPE-CT/web/modules/app/dashboard/countRankedItems
  - functions/LPE-CT/web/modules/app/dashboard/extractThreatLabel
  - functions/LPE-CT/web/modules/app/dashboard/itemIsSecurityFlagged
  - functions/LPE-CT/web/modules/app/dashboard/buildTrafficSeries
  - functions/LPE-CT/web/modules/app/dashboard/buildScanSummary
  - functions/LPE-CT/web/modules/app/ui/setText
  - functions/LPE-CT/web/modules/app/system/renderHostClock
  - functions/LPE-CT/web/modules/app/format/formatNumber
  - functions/LPE-CT/web/modules/app/format/formatBooleanLabel
  - functions/LPE-CT/web/modules/app/format/formatDateTime
  - functions/LPE-CT/web/modules/app/ui/renderMetric
  - functions/LPE-CT/web/modules/app/dashboard/renderSystemOverview
  - functions/LPE-CT/web/modules/app/dashboard/renderDashboardDetailTable
  - functions/LPE-CT/web/modules/app/format/formatMetric
  - functions/LPE-CT/web/modules/app/dashboard/buildStatusTile
  - functions/LPE-CT/web/modules/app/format/formatDurationMinutes
  - functions/LPE-CT/web/modules/app/dashboard/buildRankedRows
  - functions/LPE-CT/web/modules/app/dashboard/renderSevenDayTable
  called_by:
  - functions/LPE-CT/web/app/syncLoadingState
  - functions/LPE-CT/web/app/renderDashboard
  - functions/LPE-CT/web/app/hydrateLocaleSpecificState
---

# Signature

`function renderOverview()`

# Calls

- [buildLoadingRows](../../../../../../functions/LPE-CT/web/modules/app/ui/buildLoadingRows.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [getOperatorEmail](../../../../../../functions/LPE-CT/web/modules/app/format/getOperatorEmail.md)
- [getDigestSettings](../../../../../../functions/LPE-CT/web/modules/app/format/getDigestSettings.md)
- [getTrafficRecords](../../../../../../functions/LPE-CT/web/modules/app/format/getTrafficRecords.md)
- [healthPosture](../../../../../../functions/LPE-CT/web/modules/app/format/healthPosture.md)
- [countRankedItems](../../../../../../functions/LPE-CT/web/modules/app/dashboard/countRankedItems.md)
- [extractThreatLabel](../../../../../../functions/LPE-CT/web/modules/app/dashboard/extractThreatLabel.md)
- [itemIsSecurityFlagged](../../../../../../functions/LPE-CT/web/modules/app/dashboard/itemIsSecurityFlagged.md)
- [buildTrafficSeries](../../../../../../functions/LPE-CT/web/modules/app/dashboard/buildTrafficSeries.md)
- [buildScanSummary](../../../../../../functions/LPE-CT/web/modules/app/dashboard/buildScanSummary.md)
- [setText](../../../../../../functions/LPE-CT/web/modules/app/ui/setText.md)
- [renderHostClock](../../../../../../functions/LPE-CT/web/modules/app/system/renderHostClock.md)
- [formatNumber](../../../../../../functions/LPE-CT/web/modules/app/format/formatNumber.md)
- [formatBooleanLabel](../../../../../../functions/LPE-CT/web/modules/app/format/formatBooleanLabel.md)
- [formatDateTime](../../../../../../functions/LPE-CT/web/modules/app/format/formatDateTime.md)
- [renderMetric](../../../../../../functions/LPE-CT/web/modules/app/ui/renderMetric.md)
- [renderSystemOverview](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderSystemOverview.md)
- [renderDashboardDetailTable](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderDashboardDetailTable.md)
- [formatMetric](../../../../../../functions/LPE-CT/web/modules/app/format/formatMetric.md)
- [buildStatusTile](../../../../../../functions/LPE-CT/web/modules/app/dashboard/buildStatusTile.md)
- [formatDurationMinutes](../../../../../../functions/LPE-CT/web/modules/app/format/formatDurationMinutes.md)
- [buildRankedRows](../../../../../../functions/LPE-CT/web/modules/app/dashboard/buildRankedRows.md)
- [renderSevenDayTable](../../../../../../functions/LPE-CT/web/modules/app/dashboard/renderSevenDayTable.md)

# Called by

- [syncLoadingState](../../../../../../functions/LPE-CT/web/app/syncLoadingState.md)
- [renderDashboard](../../../../../../functions/LPE-CT/web/app/renderDashboard.md)
- [hydrateLocaleSpecificState](../../../../../../functions/LPE-CT/web/app/hydrateLocaleSpecificState.md)