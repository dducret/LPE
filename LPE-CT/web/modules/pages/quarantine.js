import { createPageModule } from "./page-module.js";

export const quarantinePage = createPageModule({
  id: "quarantine",
  labelKey: "navQuarantine",
  captionKey: "navQuarantineCaption",
  sectionIds: ["quarantine-section"],
  rendererKeys: ["quarantine"],
});
