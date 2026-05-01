import { createPageModule } from "./page-module.js";

export const filteringPage = createPageModule({
  id: "filtering",
  labelKey: "navFiltering",
  captionKey: "navFilteringCaption",
  sectionIds: ["virus-filtering-section", "address-policy-section", "attachment-policy-section", "verification-section", "dkim-section"],
  rendererKeys: ["virusFiltering", "filteringPolicy", "addressRules", "attachmentRules", "recipientVerification", "dkim"],
});
