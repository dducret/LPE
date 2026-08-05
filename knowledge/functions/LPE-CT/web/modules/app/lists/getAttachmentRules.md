---
type: JavaScript Function
title: getAttachmentRules
resource: LPE-CT/web/modules/app/lists.js#L43-L68
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/web/modules/app/lists/findAttachmentRule
  - functions/LPE-CT/web/modules/app/lists/renderAttachmentRules
---

# Signature

`function getAttachmentRules(policies = state.dashboard?.policies)`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [findAttachmentRule](../../../../../../functions/LPE-CT/web/modules/app/lists/findAttachmentRule.md)
- [renderAttachmentRules](../../../../../../functions/LPE-CT/web/modules/app/lists/renderAttachmentRules.md)