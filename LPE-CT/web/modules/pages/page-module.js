export function createPageModule({ id, labelKey, captionKey, sectionIds, rendererKeys }) {
  return Object.freeze({
    id,
    labelKey,
    captionKey,
    sectionIds: Object.freeze(sectionIds),
    rendererKeys: Object.freeze(rendererKeys),
    ownsView(view) {
      return view?.dataset?.pageView === id;
    },
    render(renderers) {
      rendererKeys.forEach((key) => {
        renderers[key]?.();
      });
    },
  });
}
