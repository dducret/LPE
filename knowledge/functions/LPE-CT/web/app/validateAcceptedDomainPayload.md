---
type: JavaScript Function
title: validateAcceptedDomainPayload
resource: LPE-CT/web/app.js#L196-L209
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/policy-drawers/isValidDomain
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/currentAcceptedDomains
  - functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain
  called_by:
  - functions/LPE-CT/web/app/openAcceptedDomainDrawer
  - functions/LPE-CT/web/app/openAcceptedDomainImportDrawer
---

# Signature

`function validateAcceptedDomainPayload(payload, existingId = null)`

# Calls

- [isValidDomain](../../../../functions/LPE-CT/web/modules/app/policy-drawers/isValidDomain.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [currentAcceptedDomains](../../../../functions/LPE-CT/web/app/currentAcceptedDomains.md)
- [normalizeDomain](../../../../functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain.md)

# Called by

- [openAcceptedDomainDrawer](../../../../functions/LPE-CT/web/app/openAcceptedDomainDrawer.md)
- [openAcceptedDomainImportDrawer](../../../../functions/LPE-CT/web/app/openAcceptedDomainImportDrawer.md)