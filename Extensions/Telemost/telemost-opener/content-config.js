(() => {
  'use strict';
  let config = null;
  let generation = 0;
  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === 'local' && changes.effectiveConfig) {
      generation++;
      config = changes.effectiveConfig.newValue || null;
    }
  });
  const requestedAt = generation;
  const ready = chrome.runtime.sendMessage({ type: 'get-config' }).then(result => {
    if (generation === requestedAt) config = result?.config || null;
  }).catch(() => { /* On extension reload, let native navigation handle the click. */ });
  globalThis.RedirectSettings = Object.freeze({ ready, toAppUrl: (value, base) => TelemostRouter.toAppUrl(value, base, config) });
})();
