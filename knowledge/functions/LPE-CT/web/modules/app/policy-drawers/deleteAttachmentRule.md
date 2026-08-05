---
type: JavaScript Function
title: deleteAttachmentRule
resource: LPE-CT/web/modules/app/policy-drawers.js#L256-L266
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/lists/findAttachmentRule
  - functions/LPE-CT/web/modules/app/format/currentPolicies
  - functions/LPE-CT/web/modules/app/lists/routeToAttachmentPolicies
  - functions/LPE-CT/web/app/savePolicies
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function deleteAttachmentRule(ruleId)`

# Calls

- [findAttachmentRule](../../../../../../functions/LPE-CT/web/modules/app/lists/findAttachmentRule.md)
- [currentPolicies](../../../../../../functions/LPE-CT/web/modules/app/format/currentPolicies.md)
- [routeToAttachmentPolicies](../../../../../../functions/LPE-CT/web/modules/app/lists/routeToAttachmentPolicies.md)
- [savePolicies](../../../../../../functions/LPE-CT/web/app/savePolicies.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)