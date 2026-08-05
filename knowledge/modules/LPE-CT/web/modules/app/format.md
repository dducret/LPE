---
type: JavaScript Module
title: format
resource: LPE-CT/web/modules/app/format.js#L1-L755
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/i18n-index-js-v-20260502-outbound-ehlo
  - external/context-js-v-20260502-outbound-ehlo
  member_of:
  - packages/LPE-CT
---

# Contains

- [buildEmptyState](../../../../../functions/LPE-CT/web/modules/app/format/buildEmptyState.md)
- [escapeHtml](../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [formatList](../../../../../functions/LPE-CT/web/modules/app/format/formatList.md)
- [isValidHostname](../../../../../functions/LPE-CT/web/modules/app/format/isValidHostname.md)
- [parseProviderChain](../../../../../functions/LPE-CT/web/modules/app/format/parseProviderChain.md)
- [antivirusProviderChain](../../../../../functions/LPE-CT/web/modules/app/format/antivirusProviderChain.md)
- [labelForAntivirusProvider](../../../../../functions/LPE-CT/web/modules/app/format/labelForAntivirusProvider.md)
- [formatAntivirusProviders](../../../../../functions/LPE-CT/web/modules/app/format/formatAntivirusProviders.md)
- [formatNumber](../../../../../functions/LPE-CT/web/modules/app/format/formatNumber.md)
- [formatScore](../../../../../functions/LPE-CT/web/modules/app/format/formatScore.md)
- [formatDetailedScore](../../../../../functions/LPE-CT/web/modules/app/format/formatDetailedScore.md)
- [formatDateTime](../../../../../functions/LPE-CT/web/modules/app/format/formatDateTime.md)
- [parseHistoryTimestamp](../../../../../functions/LPE-CT/web/modules/app/format/parseHistoryTimestamp.md)
- [formatHistoryDateTime](../../../../../functions/LPE-CT/web/modules/app/format/formatHistoryDateTime.md)
- [displayTraceId](../../../../../functions/LPE-CT/web/modules/app/format/displayTraceId.md)
- [displayClientAddress](../../../../../functions/LPE-CT/web/modules/app/format/displayClientAddress.md)
- [displayMailAddress](../../../../../functions/LPE-CT/web/modules/app/format/displayMailAddress.md)
- [historySizeBytes](../../../../../functions/LPE-CT/web/modules/app/format/historySizeBytes.md)
- [formatLongTraceDateTime](../../../../../functions/LPE-CT/web/modules/app/format/formatLongTraceDateTime.md)
- [traceHeaderValue](../../../../../functions/LPE-CT/web/modules/app/format/traceHeaderValue.md)
- [traceHeadersText](../../../../../functions/LPE-CT/web/modules/app/format/traceHeadersText.md)
- [traceTextValue](../../../../../functions/LPE-CT/web/modules/app/format/traceTextValue.md)
- [traceObjectValue](../../../../../functions/LPE-CT/web/modules/app/format/traceObjectValue.md)
- [traceContentClassification](../../../../../functions/LPE-CT/web/modules/app/format/traceContentClassification.md)
- [traceBooleanLabel](../../../../../functions/LPE-CT/web/modules/app/format/traceBooleanLabel.md)
- [tracePolicyFlag](../../../../../functions/LPE-CT/web/modules/app/format/tracePolicyFlag.md)
- [traceMessageSize](../../../../../functions/LPE-CT/web/modules/app/format/traceMessageSize.md)
- [traceAttachmentItems](../../../../../functions/LPE-CT/web/modules/app/format/traceAttachmentItems.md)
- [formatShortDate](../../../../../functions/LPE-CT/web/modules/app/format/formatShortDate.md)
- [formatMetric](../../../../../functions/LPE-CT/web/modules/app/format/formatMetric.md)
- [formatPercent](../../../../../functions/LPE-CT/web/modules/app/format/formatPercent.md)
- [formatBytes](../../../../../functions/LPE-CT/web/modules/app/format/formatBytes.md)
- [formatCompactBytes](../../../../../functions/LPE-CT/web/modules/app/format/formatCompactBytes.md)
- [firstRecipient](../../../../../functions/LPE-CT/web/modules/app/format/firstRecipient.md)
- [humanizeStatus](../../../../../functions/LPE-CT/web/modules/app/format/humanizeStatus.md)
- [formatHistoryType](../../../../../functions/LPE-CT/web/modules/app/format/formatHistoryType.md)
- [historyColumns](../../../../../functions/LPE-CT/web/modules/app/format/historyColumns.md)
- [quarantineTraceId](../../../../../functions/LPE-CT/web/modules/app/format/quarantineTraceId.md)
- [quarantineDate](../../../../../functions/LPE-CT/web/modules/app/format/quarantineDate.md)
- [quarantineScoreValue](../../../../../functions/LPE-CT/web/modules/app/format/quarantineScoreValue.md)
- [traceQueueCanBeDeleted](../../../../../functions/LPE-CT/web/modules/app/format/traceQueueCanBeDeleted.md)
- [quarantineColumns](../../../../../functions/LPE-CT/web/modules/app/format/quarantineColumns.md)
- [quarantineGridTemplate](../../../../../functions/LPE-CT/web/modules/app/format/quarantineGridTemplate.md)
- [historyGridTemplate](../../../../../functions/LPE-CT/web/modules/app/format/historyGridTemplate.md)
- [sortQuarantineItems](../../../../../functions/LPE-CT/web/modules/app/format/sortQuarantineItems.md)
- [sortHistoryItems](../../../../../functions/LPE-CT/web/modules/app/format/sortHistoryItems.md)
- [quarantineSortIndicator](../../../../../functions/LPE-CT/web/modules/app/format/quarantineSortIndicator.md)
- [sortIndicator](../../../../../functions/LPE-CT/web/modules/app/format/sortIndicator.md)
- [setQuarantineSort](../../../../../functions/LPE-CT/web/modules/app/format/setQuarantineSort.md)
- [setHistorySort](../../../../../functions/LPE-CT/web/modules/app/format/setHistorySort.md)
- [logTableState](../../../../../functions/LPE-CT/web/modules/app/format/logTableState.md)
- [logGridTemplate](../../../../../functions/LPE-CT/web/modules/app/format/logGridTemplate.md)
- [sortLogItems](../../../../../functions/LPE-CT/web/modules/app/format/sortLogItems.md)
- [logSortIndicator](../../../../../functions/LPE-CT/web/modules/app/format/logSortIndicator.md)
- [setLogSort](../../../../../functions/LPE-CT/web/modules/app/format/setLogSort.md)
- [renderLogTable](../../../../../functions/LPE-CT/web/modules/app/format/renderLogTable.md)
- [auditColumns](../../../../../functions/LPE-CT/web/modules/app/format/auditColumns.md)
- [messageLogColumns](../../../../../functions/LPE-CT/web/modules/app/format/messageLogColumns.md)
- [emailAlertLogColumns](../../../../../functions/LPE-CT/web/modules/app/format/emailAlertLogColumns.md)
- [hostLogDate](../../../../../functions/LPE-CT/web/modules/app/format/hostLogDate.md)
- [hostLogColumns](../../../../../functions/LPE-CT/web/modules/app/format/hostLogColumns.md)
- [hostLogActionButton](../../../../../functions/LPE-CT/web/modules/app/format/hostLogActionButton.md)
- [renderHostLogTable](../../../../../functions/LPE-CT/web/modules/app/format/renderHostLogTable.md)
- [formatDurationMinutes](../../../../../functions/LPE-CT/web/modules/app/format/formatDurationMinutes.md)
- [formatUptime](../../../../../functions/LPE-CT/web/modules/app/format/formatUptime.md)
- [formatReportingUptime](../../../../../functions/LPE-CT/web/modules/app/format/formatReportingUptime.md)
- [formatBooleanLabel](../../../../../functions/LPE-CT/web/modules/app/format/formatBooleanLabel.md)
- [healthPosture](../../../../../functions/LPE-CT/web/modules/app/format/healthPosture.md)
- [getOperatorEmail](../../../../../functions/LPE-CT/web/modules/app/format/getOperatorEmail.md)
- [getDigestSettings](../../../../../functions/LPE-CT/web/modules/app/format/getDigestSettings.md)
- [getTrafficRecords](../../../../../functions/LPE-CT/web/modules/app/format/getTrafficRecords.md)
- [getRelayOrPeer](../../../../../functions/LPE-CT/web/modules/app/format/getRelayOrPeer.md)
- [getPolicySignals](../../../../../functions/LPE-CT/web/modules/app/format/getPolicySignals.md)
- [dedupeList](../../../../../functions/LPE-CT/web/modules/app/format/dedupeList.md)
- [currentPolicies](../../../../../functions/LPE-CT/web/modules/app/format/currentPolicies.md)
- [currentReporting](../../../../../functions/LPE-CT/web/modules/app/format/currentReporting.md)
- [statusChipClass](../../../../../functions/LPE-CT/web/modules/app/format/statusChipClass.md)
- [labelForAddressRole](../../../../../functions/LPE-CT/web/modules/app/format/labelForAddressRole.md)
- [labelForAction](../../../../../functions/LPE-CT/web/modules/app/format/labelForAction.md)
- [labelForAttachmentScope](../../../../../functions/LPE-CT/web/modules/app/format/labelForAttachmentScope.md)
- [labelForVerificationBackend](../../../../../functions/LPE-CT/web/modules/app/format/labelForVerificationBackend.md)
- [labelForKeyStatus](../../../../../functions/LPE-CT/web/modules/app/format/labelForKeyStatus.md)

# Imports

- `../i18n/index.js?v=20260502-outbound-ehlo`
- `./context.js?v=20260502-outbound-ehlo`

# Member of

- [lpe-ct](../../../../../packages/LPE-CT.md)