---
type: JavaScript Function
title: postJson
resource: LPE-CT/web/modules/app/api.js#L73-L79
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/web/app/syncNtp
  - functions/LPE-CT/web/app/runAptUpgrade
  - functions/LPE-CT/web/app/runPowerAction
  - functions/LPE-CT/web/app/openAcceptedDomainDrawer
  - functions/LPE-CT/web/app/openAcceptedDomainImportDrawer
  - functions/LPE-CT/web/app/testAcceptedDomain
  - functions/LPE-CT/web/app/openPublicTlsUploadDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/runHealthCheck
  - functions/LPE-CT/web/modules/app/trace-actions/connectSupport
  - functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue
  - functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool
  - functions/LPE-CT/web/modules/app/trace-actions/runSpamTest
  - functions/LPE-CT/web/modules/app/trace-actions/runServiceAction
---

# Signature

`async function postJson(path, payload = null)`

# Called by

- [syncNtp](../../../../../../functions/LPE-CT/web/app/syncNtp.md)
- [runAptUpgrade](../../../../../../functions/LPE-CT/web/app/runAptUpgrade.md)
- [runPowerAction](../../../../../../functions/LPE-CT/web/app/runPowerAction.md)
- [openAcceptedDomainDrawer](../../../../../../functions/LPE-CT/web/app/openAcceptedDomainDrawer.md)
- [openAcceptedDomainImportDrawer](../../../../../../functions/LPE-CT/web/app/openAcceptedDomainImportDrawer.md)
- [testAcceptedDomain](../../../../../../functions/LPE-CT/web/app/testAcceptedDomain.md)
- [openPublicTlsUploadDrawer](../../../../../../functions/LPE-CT/web/app/openPublicTlsUploadDrawer.md)
- [runHealthCheck](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runHealthCheck.md)
- [connectSupport](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/connectSupport.md)
- [flushMailQueue](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue.md)
- [runDiagnosticTool](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool.md)
- [runSpamTest](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runSpamTest.md)
- [runServiceAction](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runServiceAction.md)