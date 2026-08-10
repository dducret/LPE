---
type: Rust Method
title: includes
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L258-L268
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_matches
  called_by:
  - functions/LPE-CT/web/app/smoke/test/createContext
  - functions/LPE-CT/web/app/smoke/test/main
  - functions/LPE-CT/web/i18n/resolveBrowserLocale
  - functions/LPE-CT/web/i18n/createI18n
  - functions/LPE-CT/web/i18n/setLocale
  - functions/LPE-CT/web/modules/app/api/parseError
  - functions/LPE-CT/web/modules/app/format/isValidHostname
  - functions/LPE-CT/web/modules/app/format/traceContentClassification
  - functions/LPE-CT/web/modules/app/format/tracePolicyFlag
  - functions/LPE-CT/web/modules/app/format/traceQueueCanBeDeleted
  - functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer
  - functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineDetails
  - functions/LPE-CT/web/modules/app/trace-actions/setQuarantineDialogTab
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_included
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
  - functions/web/admin/src/StorageManagement/isGlobalAdmin
  - functions/web/admin/src/StorageManagement/statusClass
  - functions/web/admin/src/App
  - functions/web/admin/src/mutate
  - functions/web/admin/src/runPstJobs
  - functions/web/admin/src/uploadPstImport
  - functions/web/admin/src/createSnapshot
  - functions/web/admin/src/deleteSnapshot
  - functions/web/admin/src/restoreSnapshot
  - functions/web/client/src/App/App
  - functions/web/client/src/client-helpers/filterMessages
  - functions/web/client/src/client-helpers/filterContacts
  - functions/web/client/src/client-helpers/filterTasks
  - functions/web/client/src/client-helpers/filterNotes
  - functions/web/client/src/client-helpers/filterJournalEntries
  - functions/web/client/src/components/EventEditor/addResource
  - functions/web/client/src/useClientWorkspace/mapSubmitError
  - functions/web/client/src/useClientWorkspace/useClientWorkspace
  - functions/web/shared/src/i18n/isLocale
---

# Signature

`pub(crate) fn includes(self, property_tag: u32) -> bool`

# Calls

- [property_tag_matches](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_matches.md)

# Called by

- [createContext](../../../../../../functions/LPE-CT/web/app/smoke/test/createContext.md)
- [main](../../../../../../functions/LPE-CT/web/app/smoke/test/main.md)
- [resolveBrowserLocale](../../../../../../functions/LPE-CT/web/i18n/resolveBrowserLocale.md)
- [createI18n](../../../../../../functions/LPE-CT/web/i18n/createI18n.md)
- [setLocale](../../../../../../functions/LPE-CT/web/i18n/setLocale.md)
- [parseError](../../../../../../functions/LPE-CT/web/modules/app/api/parseError.md)
- [isValidHostname](../../../../../../functions/LPE-CT/web/modules/app/format/isValidHostname.md)
- [traceContentClassification](../../../../../../functions/LPE-CT/web/modules/app/format/traceContentClassification.md)
- [tracePolicyFlag](../../../../../../functions/LPE-CT/web/modules/app/format/tracePolicyFlag.md)
- [traceQueueCanBeDeleted](../../../../../../functions/LPE-CT/web/modules/app/format/traceQueueCanBeDeleted.md)
- [openAddressRuleDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer.md)
- [renderQuarantineDetails](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/renderQuarantineDetails.md)
- [setQuarantineDialogTab](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/setQuarantineDialogTab.md)
- [fast_transfer_property_included](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_included.md)
- [write_fast_transfer_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content.md)
- [write_fast_transfer_special_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)
- [isGlobalAdmin](../../../../../../functions/web/admin/src/StorageManagement/isGlobalAdmin.md)
- [statusClass](../../../../../../functions/web/admin/src/StorageManagement/statusClass.md)
- [App](../../../../../../functions/web/admin/src/App.md)
- [mutate](../../../../../../functions/web/admin/src/mutate.md)
- [runPstJobs](../../../../../../functions/web/admin/src/runPstJobs.md)
- [uploadPstImport](../../../../../../functions/web/admin/src/uploadPstImport.md)
- [createSnapshot](../../../../../../functions/web/admin/src/createSnapshot.md)
- [deleteSnapshot](../../../../../../functions/web/admin/src/deleteSnapshot.md)
- [restoreSnapshot](../../../../../../functions/web/admin/src/restoreSnapshot.md)
- [App](../../../../../../functions/web/client/src/App/App.md)
- [filterMessages](../../../../../../functions/web/client/src/client-helpers/filterMessages.md)
- [filterContacts](../../../../../../functions/web/client/src/client-helpers/filterContacts.md)
- [filterTasks](../../../../../../functions/web/client/src/client-helpers/filterTasks.md)
- [filterNotes](../../../../../../functions/web/client/src/client-helpers/filterNotes.md)
- [filterJournalEntries](../../../../../../functions/web/client/src/client-helpers/filterJournalEntries.md)
- [addResource](../../../../../../functions/web/client/src/components/EventEditor/addResource.md)
- [mapSubmitError](../../../../../../functions/web/client/src/useClientWorkspace/mapSubmitError.md)
- [useClientWorkspace](../../../../../../functions/web/client/src/useClientWorkspace/useClientWorkspace.md)
- [isLocale](../../../../../../functions/web/shared/src/i18n/isLocale.md)