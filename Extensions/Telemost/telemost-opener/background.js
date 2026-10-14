"use strict";
importScripts("url-router.js", "config-core.js", "user-rules-file.js", "config-service.js");

const handoffBase = chrome.runtime.getURL("handoff.html");
const pending = new Map();
const intents = new Map();
const sources = new Map();
const restoring = new Set();
const lastPages = new Map();
const taskKey = token => `handoff:${token}`;
const pageKey = id => `page:${id}`;
const isHandoff = url => typeof url === "string" && url.startsWith(`${handoffBase}#`);
const isEmpty = url => !url || /^(about:blank|(?:chrome|edge|browser):\/\/(?:newtab|new-tab-page)\/?|chrome-search:\/\/local-ntp\/local-ntp.html)$/.test(url);
function needsStableHandoff(value, appUrl) {
  // Staff is also reached through automatic Messenger redirects. Reloading
  // their source starts the same redirect again, cancelling the native prompt
  // and creating an unbounded launch/restore loop. Keep the helper as the host.
  // A custom Messenger -> Telemost staff rule has the same redirect risk.
  const urls = [value];
  if (appUrl.startsWith("telemost://ychat/")) urls.push("https://" + appUrl.slice("telemost://ychat/".length));
  return urls.some(candidate => {
    const url = TelemostRouter.webUrl(candidate);
    return url && ["https://telemost.yandex.ru", "https://telemost.360.yandex.ru"].includes(url.origin) &&
      url.pathname.startsWith("/staff/");
  });
}

const initialized = chrome.storage.session.get(null).then(async saved => {
  await ConfigService.ready;
  for (const [key, value] of Object.entries(saved)) {
    if (key.startsWith("page:")) lastPages.set(Number(key.slice(5)), value);
  }
  for (const tab of await chrome.tabs.query({})) {
    if (tab.url && !isHandoff(tab.url) && !ConfigService.toAppUrl(tab.url)) {
      lastPages.set(tab.id, tab.url);
    }
  }
});

async function remember(details) {
  await ConfigService.ready;
  if (details.frameId !== 0 || isHandoff(details.url) || ConfigService.toAppUrl(details.url)) return;
  lastPages.set(details.tabId, details.url);
  chrome.storage.session.set({ [pageKey(details.tabId)]: details.url }).catch(() => {});
}

async function intercept(details) {
  if (details.frameId !== 0 || details.tabId < 0 || restoring.has(details.tabId)) return;
  if (isHandoff(details.url) || !/^https?:/.test(details.url)) return;
  const intent = {};
  intents.set(details.tabId, intent);
  const initialTab = chrome.tabs.get(details.tabId).catch(() => null);
  await ConfigService.ready;
  if (intents.get(details.tabId) !== intent) return;
  const appUrl = ConfigService.toAppUrl(details.url);
  const old = pending.get(details.tabId);
  if (!appUrl) {
    if (old) {
      pending.delete(details.tabId);
      chrome.storage.session.remove(taskKey(old.token)).catch(() => {});
    }
    return;
  }
  if (old?.url === details.url) return;
  if (old) chrome.storage.session.remove(taskKey(old.token)).catch(() => {});
  const task = { token: crypto.randomUUID(), tabId: details.tabId, url: details.url, appUrl };
  pending.set(details.tabId, task);
  try {
    await initialized;
    const tab = await initialTab;
    if (!tab) return;
    if (pending.get(details.tabId) !== task) return;
    const prior = (!ConfigService.toAppUrl(tab.url) && !isHandoff(tab.url) && tab.url) || lastPages.get(tab.id);
    task.returnUrl = isEmpty(prior) ? null : prior;
    // A blank/new tab contains no previous page to preserve. Existing pages and
    // pinned tabs are never automatically closed by this extension.
    task.closeTab = !task.returnUrl && !tab.pinned;
    task.sourceId = sources.get(tab.id) ?? tab.openerTabId ?? null;
    await chrome.storage.session.set({ [taskKey(task.token)]: task });
    if (pending.get(details.tabId) !== task) {
      await chrome.storage.session.remove(taskKey(task.token));
      return;
    }
    await chrome.tabs.update(tab.id, { url: `${handoffBase}#${task.token}` });
  } catch {
    if (pending.get(details.tabId) === task) pending.delete(details.tabId);
    await chrome.storage.session.remove(taskKey(task.token)).catch(() => {});
  }
}

async function launchExternal(tabId, appUrl, activate) {
  await chrome.tabs.update(tabId, { url: appUrl, ...(activate ? { active: true } : {}) });
}

async function isCurrent(task) {
  try {
    const tab = await chrome.tabs.get(task.tabId);
    return tab.url === `${handoffBase}#${task.token}` &&
      (!tab.pendingUrl || tab.pendingUrl === tab.url);
  } catch { return false; }
}

// Await actual navigation rather than assuming that tabs.goBack's promise means
// the previous document has already been restored.
function waitForNavigation(tabId, start) {
  return new Promise((resolve, reject) => {
    const done = details => {
      if (details.tabId === tabId && details.frameId === 0) finish(null, details.url);
    };
    const timer = setTimeout(() => finish(new Error("Не удалось вернуться на исходную страницу.")), 8000);
    function finish(error, url) {
      clearTimeout(timer);
      chrome.webNavigation.onCommitted.removeListener(done);
      chrome.webNavigation.onReferenceFragmentUpdated.removeListener(done);
      error ? reject(error) : resolve(url);
    }
    chrome.webNavigation.onCommitted.addListener(done);
    chrome.webNavigation.onReferenceFragmentUpdated.addListener(done);
    start().catch(error => finish(error));
  });
}

async function complete(task) {
  await ConfigService.ready;
  if (ConfigService.toAppUrl(task.url) !== task.appUrl) throw new Error("Правило отключено или изменилось. Повторите переход.");
  const target = await chrome.tabs.get(task.tabId);
  if (!await isCurrent(task)) throw new Error("Вкладка уже перешла на другую страницу.");
  if (task.closeTab) {
    const tabs = await chrome.tabs.query({ windowId: target.windowId });
    const available = tabs.filter(tab => tab.id !== target.id && !tab.discarded &&
      !tab.pendingUrl && tab.status === "complete" && !isEmpty(tab.url) && !isHandoff(tab.url) &&
      !ConfigService.toAppUrl(tab.url));
    const anchor = available.find(tab => tab.id === task.sourceId) ||
      available.find(tab => tab.active) || available[0];
    if (anchor) {
      // The external-app prompt belongs to the surviving tab. Removing the
      // destination therefore cannot dismiss that prompt or cancel its launch.
      await launchExternal(anchor.id, task.appUrl, target.active);
      if (await isCurrent(task)) await chrome.tabs.remove(target.id);
      return { closed: true };
    }
  }
  if (task.returnUrl && !needsStableHandoff(task.url, task.appUrl)) {
    restoring.add(task.tabId);
    try {
      let url;
      try { url = await waitForNavigation(task.tabId, () => chrome.tabs.goBack(task.tabId)); } catch { /* Reload the saved page below if history is unavailable. */ }
      // A fast HTTPS response may commit before the background worker replaces
      // it with handoff.html. Skip that extra history entry too.
      if (ConfigService.toAppUrl(url)) {
        try { url = await waitForNavigation(task.tabId, () => chrome.tabs.goBack(task.tabId)); } catch { /* Fall back to the saved URL. */ }
      }
      // Chrome's Back action can skip entries without a user interaction.
      // Restore the saved URL explicitly when the browser skipped the source.
      if (url !== task.returnUrl) {
        await waitForNavigation(task.tabId, () => chrome.tabs.update(task.tabId, { url: task.returnUrl }));
      }
      await launchExternal(task.tabId, task.appUrl, false);
      return { restored: true };
    } finally { restoring.delete(task.tabId); }
  }
  // With only one tab there is nowhere else to host the browser confirmation.
  // Keep a usable page instead of dismissing the prompt by closing its tab.
  await launchExternal(task.tabId, task.appUrl, false);
  return { kept: true };
}

chrome.webNavigation.onBeforeNavigate.addListener(details => { void intercept(details); });
chrome.webNavigation.onReferenceFragmentUpdated.addListener(details => { void intercept(details); });
chrome.webNavigation.onCommitted.addListener(details => { void remember(details); });
chrome.webNavigation.onCreatedNavigationTarget.addListener(details => {
  sources.set(details.tabId, details.sourceTabId);
});
chrome.tabs.onRemoved.addListener(tabId => {
  const task = pending.get(tabId);
  pending.delete(tabId);
  intents.delete(tabId);
  lastPages.delete(tabId);
  sources.delete(tabId);
  const keys = [pageKey(tabId)];
  if (task) keys.push(taskKey(task.token));
  chrome.storage.session.remove(keys).catch(() => {});
});

const completing = new Set();
chrome.runtime.onMessage.addListener((message, sender, respond) => {
  if (message?.type !== "handoff-ready" || !sender.tab || sender.id !== chrome.runtime.id ||
      sender.frameId !== 0 || !isHandoff(sender.url)) return;
  // After a fragment-only change Chromium can report the document's original
  // URL in sender.url. Validate the helper here; complete() checks the token
  // against the actual current tab URL and the task's owning tab before launch.
  const token = message.token;
  if (completing.has(token)) { respond({ error: "Запуск уже выполняется." }); return; }
  completing.add(token);
  (async () => {
    const saved = await chrome.storage.session.get(taskKey(token));
    await ConfigService.ready;
    const task = saved[taskKey(token)];
    if (!task || task.tabId !== sender.tab.id || ConfigService.toAppUrl(task.url) !== task.appUrl) {
      return { error: "Ссылка устарела. Повторите переход из исходной страницы." };
    }
    try { return await complete(task); }
    catch (error) { return { error: error.message }; }
    finally {
      if (pending.get(task.tabId)?.token === task.token) pending.delete(task.tabId);
      await chrome.storage.session.remove(taskKey(token));
    }
  })().then(respond, () => respond({ error: "Не удалось передать ссылку приложению." }))
    .finally(() => completing.delete(token));
  return true;
});


const settingsPages = [chrome.runtime.getURL("options.html"), chrome.runtime.getURL("popup.html")];
chrome.runtime.onMessage.addListener((message, sender, respond) => {
  if (message?.type === "get-config" && sender.id === chrome.runtime.id) {
    ConfigService.ready.then(() => respond(ConfigService.snapshot));
    return true;
  }
  if (!["change-config", "export-files"].includes(message?.type)) return;
  if (sender.id !== chrome.runtime.id || !settingsPages.includes((sender.url || "").split(/[?#]/)[0])) {
    respond({ error: "Настройки можно менять только через окно расширения." });
    return;
  }
  const operation = message.type === "export-files" ? ConfigService.exportFiles() : ConfigService.execute(message.command);
  operation.then(respond, error => respond({ error: error.message }));
  return true;
});
