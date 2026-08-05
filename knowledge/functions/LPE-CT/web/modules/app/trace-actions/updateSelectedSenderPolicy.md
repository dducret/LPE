---
type: JavaScript Function
title: updateSelectedSenderPolicy
resource: LPE-CT/web/modules/app/trace-actions.js#L359-L389
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/lists/selectedQuarantineItems
  - functions/LPE-CT/web/modules/app/format/displayMailAddress
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  - functions/LPE-CT/web/modules/app/format/currentPolicies
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/LPE-CT/web/app/savePolicies
  - functions/LPE-CT/web/modules/app/format/labelForAction
  called_by:
  - functions/LPE-CT/web/modules/app/trace-actions/runQuarantineBulkAction
---

# Signature

`async function updateSelectedSenderPolicy(action)`

# Calls

- [selectedQuarantineItems](../../../../../../functions/LPE-CT/web/modules/app/lists/selectedQuarantineItems.md)
- [displayMailAddress](../../../../../../functions/LPE-CT/web/modules/app/format/displayMailAddress.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)
- [currentPolicies](../../../../../../functions/LPE-CT/web/modules/app/format/currentPolicies.md)
- [add](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [savePolicies](../../../../../../functions/LPE-CT/web/app/savePolicies.md)
- [labelForAction](../../../../../../functions/LPE-CT/web/modules/app/format/labelForAction.md)

# Called by

- [runQuarantineBulkAction](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runQuarantineBulkAction.md)