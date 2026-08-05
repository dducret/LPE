---
type: JavaScript Function
title: deleteAddressRule
resource: LPE-CT/web/modules/app/policy-drawers.js#L177-L187
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/lists/findAddressRule
  - functions/LPE-CT/web/modules/app/format/currentPolicies
  - functions/LPE-CT/web/modules/app/lists/routeToPolicies
  - functions/LPE-CT/web/app/savePolicies
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function deleteAddressRule(ruleId)`

# Calls

- [findAddressRule](../../../../../../functions/LPE-CT/web/modules/app/lists/findAddressRule.md)
- [currentPolicies](../../../../../../functions/LPE-CT/web/modules/app/format/currentPolicies.md)
- [routeToPolicies](../../../../../../functions/LPE-CT/web/modules/app/lists/routeToPolicies.md)
- [savePolicies](../../../../../../functions/LPE-CT/web/app/savePolicies.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)