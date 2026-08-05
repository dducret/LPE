---
type: JavaScript Function
title: classifyTrafficItem
resource: LPE-CT/web/modules/app/dashboard.js#L131-L140
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/dashboard/itemIsSpam
  - functions/LPE-CT/web/modules/app/dashboard/itemIsVirus
  - functions/LPE-CT/web/modules/app/dashboard/itemHasBannedAttachment
  - functions/LPE-CT/web/modules/app/dashboard/itemHasInvalidRecipient
  - functions/LPE-CT/web/modules/app/dashboard/itemHasRelayDenied
  - functions/LPE-CT/web/modules/app/dashboard/itemHasRblHit
  - functions/LPE-CT/web/modules/app/dashboard/itemIsRejected
  called_by:
  - functions/LPE-CT/web/modules/app/dashboard/buildScanSummary
  - functions/LPE-CT/web/modules/app/dashboard/buildTrafficSeries
---

# Signature

`function classifyTrafficItem(item)`

# Calls

- [itemIsSpam](../../../../../../functions/LPE-CT/web/modules/app/dashboard/itemIsSpam.md)
- [itemIsVirus](../../../../../../functions/LPE-CT/web/modules/app/dashboard/itemIsVirus.md)
- [itemHasBannedAttachment](../../../../../../functions/LPE-CT/web/modules/app/dashboard/itemHasBannedAttachment.md)
- [itemHasInvalidRecipient](../../../../../../functions/LPE-CT/web/modules/app/dashboard/itemHasInvalidRecipient.md)
- [itemHasRelayDenied](../../../../../../functions/LPE-CT/web/modules/app/dashboard/itemHasRelayDenied.md)
- [itemHasRblHit](../../../../../../functions/LPE-CT/web/modules/app/dashboard/itemHasRblHit.md)
- [itemIsRejected](../../../../../../functions/LPE-CT/web/modules/app/dashboard/itemIsRejected.md)

# Called by

- [buildScanSummary](../../../../../../functions/LPE-CT/web/modules/app/dashboard/buildScanSummary.md)
- [buildTrafficSeries](../../../../../../functions/LPE-CT/web/modules/app/dashboard/buildTrafficSeries.md)