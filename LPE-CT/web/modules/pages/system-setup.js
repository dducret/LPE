import { createPageModule } from "./page-module.js";

export const systemSetupPage = createPageModule({
  id: "system-setup",
  labelKey: "navSystemSetup",
  captionKey: "navSystemSetupCaption",
  sectionIds: ["platform-section"],
  rendererKeys: ["platform"],
});
