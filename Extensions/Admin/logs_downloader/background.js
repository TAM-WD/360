// background.js - фоновый сервис для работы в фоне

let activeExports = new Map(); // tabId -> exportState

// Слушаем сообщения от popup и content script
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const tabId = sender.tab?.id || message.tabId;
  
  if (message.action === 'startExport') {
    handleStartExport(tabId);
    // Устанавливаем бейдж "в процессе" - оранжевый речевой пузырь
    chrome.action.setBadgeText({ text: '●', tabId: tabId });
    chrome.action.setBadgeBackgroundColor({ color: '#ff9800', tabId: tabId });
    sendResponse({ status: 'started' });
  } else if (message.action === 'cancelExport') {
    handleCancelExport(tabId);
    sendResponse({ status: 'cancelled' });
  } else if (message.action === 'updateStatus') {
    handleStatusUpdate(tabId, message.status, message.data);
    sendResponse({ status: 'updated' });
  } else if (message.action === 'getExportState') {
    const state = activeExports.get(tabId);
    sendResponse({ state: state || null });
  } else if (message.action === 'exportComplete') {
    handleExportComplete(tabId, message.result);
    // Устанавливаем бейдж "готово"
    chrome.action.setBadgeText({ text: '✓', tabId: tabId });
    chrome.action.setBadgeBackgroundColor({ color: '#4caf50', tabId: tabId });
    sendResponse({ status: 'completed' });
  }
  return true; // Для асинхронного ответа
});

function handleStartExport(tabId) {
  activeExports.set(tabId, {
    status: 'running',
    startTime: Date.now(),
    rows: 0,
    scrolls: 0
  });
  console.log(`Export started for tab ${tabId}`);
}

function handleCancelExport(tabId) {
  const state = activeExports.get(tabId);
  if (state) {
    state.status = 'cancelled';
    console.log(`Export cancelled for tab ${tabId}`);
  }
}

function handleStatusUpdate(tabId, status, data) {
  const state = activeExports.get(tabId);
  if (state && state.status === 'running') {
    state.currentStatus = status;
    state.rows = data.rows || state.rows;
    state.scrolls = data.scrolls || state.scrolls;
  }
}

function handleExportComplete(tabId, result) {
  const state = activeExports.get(tabId);
  if (state) {
    state.status = 'completed';
    state.result = result;
    state.endTime = Date.now();
    
    // Показываем нотификацию
    if (result.success) {
      chrome.notifications.create({
        type: 'basic',
        iconUrl: 'icon128.png',
        title: 'Audit Logs Exporter',
        message: `Экспорт завершен! Собрано ${result.recordCount} записей.`,
        priority: 2
      });
    }
    
    // Сохраняем результат в chrome.storage для доступа после закрытия popup
    if (result.data) {
      const storageKey = `export_state_${tabId}`;
      chrome.storage.local.set({
        [storageKey]: {
          status: 'completed',
          data: result.data,
          recordCount: result.recordCount,
          scrollCount: result.scrollCount || 0,
          timestamp: Date.now(),
          result: result
        }
      });
    }
    
    // Очищаем состояние через 5 минут
    setTimeout(() => {
      activeExports.delete(tabId);
    }, 5 * 60 * 1000);
  }
}

// Очищаем состояние при закрытии вкладки
chrome.tabs.onRemoved.addListener((tabId) => {
  if (activeExports.has(tabId)) {
    activeExports.delete(tabId);
  }
});

console.log('Audit Logs Exporter background service worker started');
