/**
 * ISOLATED-world мост: window <-> chrome.runtime, в обе стороны.
 */
(() => {
  const BRIDGE_TAG = 'yandex-chat-members-exporter';
  const LOG_PREFIX = '[yc-exporter:content]';

  // page -> background
  window.addEventListener('message', (event) => {
    if (event.source !== window) return;
    const data = event.data;
    if (!data || data.source !== BRIDGE_TAG) return;
    if (data.direction === 'to-page') return; // наше же сообщение в обратную сторону
    try {
      chrome.runtime.sendMessage({ type: data.type, payload: data.payload })
        .catch(() => {});
    } catch (_) {}
  });

  // background -> page
  chrome.runtime.onMessage.addListener((msg) => {
    if (!msg || !msg.type) return;
    if (msg.target !== 'page') return;
    console.log(LOG_PREFIX, 'relay to page:', msg.type);
    window.postMessage(
      { source: BRIDGE_TAG, direction: 'to-page', type: msg.type, payload: msg.payload || {} },
      window.location.origin
    );
  });

  console.log(LOG_PREFIX, 'мост готов');
})();
