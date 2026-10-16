// Registered at document_start: install the network bridge before waiting for DOM/settings.
(() => {
  const navigation = new TicketNavigation();
  let settings = {hideNoSubject: false, hideArchived: false, enableNavigation: false};
  const changedSettings = new Set();
  let renderTimer = null;
  let lastUrl = location.href;
  let previousStats = '';
  let captureQueue = Promise.resolve();

  window.addEventListener('supportHelperNetwork', event => {
    let message;
    try { message = JSON.parse(event.detail); } catch { return; }
    if (!['captureTickets', 'captureTicket'].includes(message?.action)) return;
    const receivedPageUrl = location.href;
    // Preserve event order and never acknowledge a write before background has saved it.
    captureQueue = captureQueue.catch(() => {}).then(async () => {
      const response = await chrome.runtime.sendMessage({...message, receivedPageUrl});
      if (!response?.success) throw new Error(response?.error || 'Не удалось сохранить список');
      if (message.action === 'captureTicket') {
        navigation.setCurrentTicket(message.currentTicketId, message.pageUrl, message.requestedAt);
      }
      navigation.schedule();
    }).catch(error => console.error('Support Center Helper: список не сохранён', error.message));
  });
  const ready = () => window.dispatchEvent(new CustomEvent('supportHelperReady'));
  window.addEventListener('supportHelperInterceptorReady', ready);
  ready();

  function isTargetPage() { return /^\/support-center(?:\/|$)/.test(location.pathname); }
  function rows() { return [...document.querySelectorAll('tr[data-testid="resource-table-row"]')]; }
  function getStats() {
    return {
      hiddenNoSubject: document.querySelectorAll('.ext-hidden-no-subject').length,
      hiddenArchived: document.querySelectorAll('.ext-hidden-archived').length,
      totalTickets: rows().length, isTicketPage: Boolean(navigation.ticketId()),
      isTargetPage: isTargetPage(), isInitialized: true
    };
  }

  function processRows() {
    if (!document.documentElement) return;
    if (!document.getElementById('ext-hide-styles')) {
      const style = document.createElement('style');
      style.id = 'ext-hide-styles';
      style.textContent = '.ext-hidden-no-subject,.ext-hidden-archived{display:none!important}';
      (document.head || document.documentElement).append(style);
    }
    for (const row of rows()) {
      const noSubject = [...row.querySelectorAll('[data-testid="orb-text"]')]
        .some(element => element.textContent.trim() === 'Без темы');
      const status = row.querySelector('[data-testid="ticket-status-tag"] .Orb-Tag-label')?.textContent.trim();
      row.classList.toggle('ext-hidden-no-subject', isTargetPage() && settings.hideNoSubject && noSubject);
      row.classList.toggle('ext-hidden-archived', isTargetPage() && settings.hideArchived && ['Архивировано', 'Архивирован'].includes(status));
    }
    const stats = getStats();
    const signature = JSON.stringify(stats);
    if (signature !== previousStats) {
      previousStats = signature;
      chrome.runtime.sendMessage({action: 'statsUpdated', ...stats}).catch(() => {});
    }
  }

  function scheduleRows() {
    clearTimeout(renderTimer);
    renderTimer = setTimeout(processRows, 100);
  }
  function applySettings() {
    navigation.setEnabled(settings.enableNavigation);
    scheduleRows();
  }

  chrome.storage.onChanged.addListener((changes, area) => {
    if (area !== 'local') return;
    for (const key of Object.keys(settings)) {
      if (changes[key]) { settings[key] = Boolean(changes[key].newValue); changedSettings.add(key); }
    }
    applySettings();
  });
  chrome.storage.local.get(Object.keys(settings)).then(values => {
    // A storage change delivered during initialization takes precedence over an older snapshot.
    for (const key of Object.keys(settings)) {
      if (!changedSettings.has(key)) settings[key] = Boolean(values[key]);
    }
    applySettings();
  }).catch(error => console.error('Support Center Helper: настройки не загружены', error.message));

  chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (message.action === 'getStats') { sendResponse(getStats()); return false; }
    if (message.action === 'getNavigationDiagnostics') { sendResponse(navigation.diagnostics()); return false; }
    if (message.action === 'ticketsUpdated') navigation.schedule();
    if (message.action === 'toggleFeature') {
      const key = {noSubject: 'hideNoSubject', archived: 'hideArchived', navigation: 'enableNavigation'}[message.feature];
      if (key) { settings[key] = Boolean(message.enabled); applySettings(); }
      sendResponse({success: true});
    }
    return false;
  });

  new MutationObserver(scheduleRows).observe(document, {childList: true, subtree: true, characterData: true});
  window.addEventListener('pageshow', () => { scheduleRows(); navigation.schedule(); });
  // SPA history calls happen in MAIN; polling only the URL also covers changes without DOM mutations.
  setInterval(() => {
    if (lastUrl !== location.href) { lastUrl = location.href; scheduleRows(); navigation.schedule(); }
  }, 500);
  navigation.start();
  scheduleRows();
})();
