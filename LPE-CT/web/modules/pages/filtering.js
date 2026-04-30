import { createPageModule } from "./page-module.js";

export const filteringPage = createPageModule({
  id: "filtering",
  labelKey: "navFiltering",
  captionKey: "navFilteringCaption",
  sectionIds: ["address-policy-section", "attachment-policy-section", "verification-section", "dkim-section"],
  rendererKeys: ["filteringPolicy", "addressRules", "attachmentRules", "recipientVerification", "dkim"],
});
