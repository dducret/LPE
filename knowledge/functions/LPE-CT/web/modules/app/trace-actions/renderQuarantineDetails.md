---
type: JavaScript Function
title: renderQuarantineDetails
resource: LPE-CT/web/modules/app/trace-actions.js#L40-L77
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/displayMailAddress
  - functions/LPE-CT/web/modules/app/format/traceObjectValue
  - functions/LPE-CT/web/modules/app/format/formatLongTraceDateTime
  - functions/LPE-CT/web/modules/app/format/quarantineDate
  - functions/LPE-CT/web/modules/app/format/traceHeaderValue
  - functions/LPE-CT/web/modules/app/format/formatList
  - functions/LPE-CT/web/modules/app/format/displayClientAddress
  - functions/LPE-CT/web/modules/app/format/formatNumber
  - functions/LPE-CT/web/modules/app/format/traceMessageSize
  - functions/LPE-CT/web/modules/app/format/traceContentClassification
  - functions/LPE-CT/web/modules/app/format/traceBooleanLabel
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  - functions/LPE-CT/web/modules/app/format/formatDetailedScore
  - functions/LPE-CT/web/modules/app/format/quarantineScoreValue
  - functions/LPE-CT/web/modules/app/format/humanizeStatus
  - functions/LPE-CT/web/modules/app/format/tracePolicyFlag
  - functions/LPE-CT/web/modules/app/trace-actions/quarantineDetailRows
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineTraceDialog
---

# Signature

`function renderQuarantineDetails(trace, current, retainedHistory)`

# Calls

- [displayMailAddress](../../../../../../functions/LPE-CT/web/modules/app/format/displayMailAddress.md)
- [traceObjectValue](../../../../../../functions/LPE-CT/web/modules/app/format/traceObjectValue.md)
- [formatLongTraceDateTime](../../../../../../functions/LPE-CT/web/modules/app/format/formatLongTraceDateTime.md)
- [quarantineDate](../../../../../../functions/LPE-CT/web/modules/app/format/quarantineDate.md)
- [traceHeaderValue](../../../../../../functions/LPE-CT/web/modules/app/format/traceHeaderValue.md)
- [formatList](../../../../../../functions/LPE-CT/web/modules/app/format/formatList.md)
- [displayClientAddress](../../../../../../functions/LPE-CT/web/modules/app/format/displayClientAddress.md)
- [formatNumber](../../../../../../functions/LPE-CT/web/modules/app/format/formatNumber.md)
- [traceMessageSize](../../../../../../functions/LPE-CT/web/modules/app/format/traceMessageSize.md)
- [traceContentClassification](../../../../../../functions/LPE-CT/web/modules/app/format/traceContentClassification.md)
- [traceBooleanLabel](../../../../../../functions/LPE-CT/web/modules/app/format/traceBooleanLabel.md)
- [includes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)
- [formatDetailedScore](../../../../../../functions/LPE-CT/web/modules/app/format/formatDetailedScore.md)
- [quarantineScoreValue](../../../../../../functions/LPE-CT/web/modules/app/format/quarantineScoreValue.md)
- [humanizeStatus](../../../../../../functions/LPE-CT/web/modules/app/format/humanizeStatus.md)
- [tracePolicyFlag](../../../../../../functions/LPE-CT/web/modules/app/format/tracePolicyFlag.md)
- [quarantineDetailRows](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/quarantineDetailRows.md)

# Called by

- [renderQuarantineTraceDialog](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineTraceDialog.md)