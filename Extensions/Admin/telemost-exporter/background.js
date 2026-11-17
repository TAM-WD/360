// Observe only the organization and originating frame. Never read headers/cookies.
let queue = Promise.resolve();
chrome.webRequest.onBeforeRequest.addListener(details => {
  if (details.method !== 'GET' || details.tabId < 0) return;
  const url = new URL(details.url);
  if (url.pathname !== '/telemost_admin_front/v1/telemost-api/admin/conferences') return;
  if (!['https://admin.telemost.yandex.ru', 'https://admin.yandex.ru'].includes(details.initiator)) return;
  const orgId = url.searchParams.get('org_id');
  if (!/^\d+$/.test(orgId || '')) return;
  const context = {orgId, tabId: details.tabId, frameId: details.frameId, observedAt: Date.now()};
  queue = queue.catch(() => {}).then(async () => {
    const {contexts = []} = await chrome.storage.session.get('contexts');
    const next = contexts.filter(c => !(c.tabId === context.tabId && c.frameId === context.frameId));
    next.unshift(context);
    await chrome.storage.session.set({contexts: next.slice(0, 20)});
  });
}, {urls: ['https://cloud-api.yandex.ru/telemost_admin_front/v1/telemost-api/admin/conferences*']});

chrome.runtime.onMessage.addListener((message, sender, respond) => {
  if (sender.id !== chrome.runtime.id || sender.tab || message?.type !== 'CONTEXTS') return;
  queue.catch(() => {}).then(async () => {
    const {contexts = []} = await chrome.storage.session.get('contexts');
    const live = [];
    for (const context of contexts) {
      try {
        const tab = await chrome.tabs.get(context.tabId);
        if (/^https:\/\/admin\.(?:telemost\.)?yandex\.ru\//.test(tab.url || '')) live.push(context);
      } catch { /* Closed tab. */ }
    }
    respond({contexts: live});
  }).catch(() => respond({contexts: []}));
  return true;
});
