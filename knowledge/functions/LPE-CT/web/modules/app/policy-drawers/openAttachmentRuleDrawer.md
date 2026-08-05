---
type: JavaScript Function
title: openAttachmentRuleDrawer
resource: LPE-CT/web/modules/app/policy-drawers.js#L189-L254
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/lists/findAttachmentRule
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/modules/app/policy-drawers/isValidMimeType
  - functions/LPE-CT/web/modules/app/format/currentPolicies
  - functions/LPE-CT/web/modules/app/lists/routeToAttachmentPolicies
  - functions/LPE-CT/web/app/savePolicies
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`function openAttachmentRuleDrawer(ruleId = null, opener = document.activeElement)`

# Calls

- [findAttachmentRule](../../../../../../functions/LPE-CT/web/modules/app/lists/findAttachmentRule.md)
- [renderDrawerForm](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [isValidMimeType](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/isValidMimeType.md)
- [currentPolicies](../../../../../../functions/LPE-CT/web/modules/app/format/currentPolicies.md)
- [routeToAttachmentPolicies](../../../../../../functions/LPE-CT/web/modules/app/lists/routeToAttachmentPolicies.md)
- [savePolicies](../../../../../../functions/LPE-CT/web/app/savePolicies.md)
- [closeDrawer](../../../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)