// Ключи для хранения состояний
const STORAGE_KEY_NO_SUBJECT = 'hideNoSubject';
const STORAGE_KEY_ARCHIVED = 'hideArchived';

// Состояния функций
let hideNoSubjectEnabled = false;
let hideArchivedEnabled = false;

// Классы для скрытия элементов
const HIDDEN_CLASS_NO_SUBJECT = 'ext-hidden-no-subject';
const HIDDEN_CLASS_ARCHIVED = 'ext-hidden-archived';

// Флаг инициализации
let isInitialized = false;

// Проверка, находимся ли мы на нужной странице
function isTargetPage() {
  const path = window.location.pathname;
  return path.includes('/support-center');
}

// Инициализация
async function init() {
  if (isInitialized) {
    return;
  }
  
  if (!isTargetPage()) {
    setupUrlWatcher();
    return;
  }
  
  isInitialized = true;
  
  injectStyles();
  
  // Загружаем оба состояния из local storage
  try {
    const result = await chrome.storage.local.get([STORAGE_KEY_NO_SUBJECT, STORAGE_KEY_ARCHIVED]);
    hideNoSubjectEnabled = result[STORAGE_KEY_NO_SUBJECT] || false;
    hideArchivedEnabled = result[STORAGE_KEY_ARCHIVED] || false;
  } catch (error) {
    console.error('Support Center Helper: Ошибка загрузки состояний:', error);
  }
  
  await waitForPageLoad();
  
  if (hideNoSubjectEnabled || hideArchivedEnabled) {
    enableFeatures();
  }
}

// Ожидание загрузки страницы
function waitForPageLoad() {
  return new Promise((resolve) => {
    if (document.readyState === 'complete') {
      resolve();
    } else {
      window.addEventListener('load', resolve, { once: true });
    }
  });
}

// Отслеживание изменений URL для SPA
function setupUrlWatcher() {
  let lastUrl = location.href;
  
  const urlObserver = new MutationObserver(() => {
    const currentUrl = location.href;
    if (currentUrl !== lastUrl) {
      lastUrl = currentUrl;
      handleUrlChange();
    }
  });
  
  urlObserver.observe(document, {
    subtree: true,
    childList: true
  });
  
  const originalPushState = history.pushState;
  const originalReplaceState = history.replaceState;
  
  history.pushState = function(...args) {
    originalPushState.apply(this, args);
    handleUrlChange();
  };
  
  history.replaceState = function(...args) {
    originalReplaceState.apply(this, args);
    handleUrlChange();
  };
  
  window.addEventListener('popstate', handleUrlChange);
}

// Обработка изменения URL
async function handleUrlChange() {
  if (isTargetPage() && !isInitialized) {
    await init();
  } else if (!isTargetPage() && isInitialized) {
    cleanup();
  } else if (isTargetPage() && isInitialized && (hideNoSubjectEnabled || hideArchivedEnabled)) {
    setTimeout(() => {
      processRows();
    }, 1000);
  }
}

// Очистка при уходе со страницы
function cleanup() {
  disableFeatures();
  isInitialized = false;
}

// Внедрение стилей для скрытия элементов
function injectStyles() {
  if (document.getElementById('ext-hide-styles')) {
    return;
  }
  
  const style = document.createElement('style');
  style.id = 'ext-hide-styles';
  style.textContent = `
    .${HIDDEN_CLASS_NO_SUBJECT},
    .${HIDDEN_CLASS_ARCHIVED} {
      display: none !important;
    }
  `;
  
  if (document.head) {
    document.head.appendChild(style);
  } else {
    const observer = new MutationObserver((mutations, obs) => {
      if (document.head) {
        document.head.appendChild(style);
        obs.disconnect();
      }
    });
    observer.observe(document.documentElement, { childList: true });
  }
}

// Ожидание появления элементов таблицы
function waitForTable(timeout = 10000) {
  return new Promise((resolve) => {
    const existingRows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
    if (existingRows.length > 0) {
      resolve(true);
      return;
    }
    
    const startTime = Date.now();
    const observer = new MutationObserver((mutations) => {
      const rows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
      
      if (rows.length > 0) {
        observer.disconnect();
        resolve(true);
      } else if (Date.now() - startTime > timeout) {
        console.error('Support Center Helper: таймаут ожидания таблицы');
        observer.disconnect();
        resolve(false);
      }
    });
    
    observer.observe(document.body, {
      childList: true,
      subtree: true
    });
  });
}

// Получение общего количества тикетов
function getTotalTicketsCount() {
  const rows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
  return rows.length;
}

// Обработка всех строк
function processRows() {
  const rows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
  
  if (rows.length === 0) {
    return;
  }
  
  let hiddenNoSubject = 0;
  let hiddenArchived = 0;
  
  rows.forEach(row => {
    // Обработка "Без темы"
    if (hideNoSubjectEnabled) {
      const textElements = row.querySelectorAll('[data-testid="orb-text"]');
      let hasNoSubject = false;
      
      textElements.forEach(element => {
        if (element.textContent.trim() === 'Без темы') {
          hasNoSubject = true;
        }
      });
      
      if (hasNoSubject) {
        if (!row.classList.contains(HIDDEN_CLASS_NO_SUBJECT)) {
          row.classList.add(HIDDEN_CLASS_NO_SUBJECT);
          hiddenNoSubject++;
        }
      } else {
        row.classList.remove(HIDDEN_CLASS_NO_SUBJECT);
      }
    } else {
      row.classList.remove(HIDDEN_CLASS_NO_SUBJECT);
    }
    
    // Обработка архивированных
    if (hideArchivedEnabled) {
      const statusTag = row.querySelector('[data-testid="ticket-status-tag"] .Orb-Tag-label');
      let isArchived = false;
      
      if (statusTag && statusTag.textContent.trim() === 'Архивировано') {
        isArchived = true;
      }
      
      if (isArchived) {
        if (!row.classList.contains(HIDDEN_CLASS_ARCHIVED)) {
          row.classList.add(HIDDEN_CLASS_ARCHIVED);
          hiddenArchived++;
        }
      } else {
        row.classList.remove(HIDDEN_CLASS_ARCHIVED);
      }
    } else {
      row.classList.remove(HIDDEN_CLASS_ARCHIVED);
    }
    
    row.dataset.processedByExt = 'true';
  });
  
  if (hiddenNoSubject > 0 || hiddenArchived > 0) {
    notifyStatsUpdate();
  }
}

// Показ всех скрытых строк
function showAllRows() {
  const hiddenNoSubject = document.querySelectorAll(`.${HIDDEN_CLASS_NO_SUBJECT}`);
  const hiddenArchived = document.querySelectorAll(`.${HIDDEN_CLASS_ARCHIVED}`);
  
  hiddenNoSubject.forEach(row => {
    row.classList.remove(HIDDEN_CLASS_NO_SUBJECT);
    row.dataset.processedByExt = 'false';
  });
  
  hiddenArchived.forEach(row => {
    row.classList.remove(HIDDEN_CLASS_ARCHIVED);
    row.dataset.processedByExt = 'false';
  });
  
  if (hiddenNoSubject.length > 0 || hiddenArchived.length > 0) {
    notifyStatsUpdate();
  }
}

// Включение функций
async function enableFeatures() {
  const tableExists = await waitForTable();
  
  if (tableExists) {
    processRows();
  }
  
  startObserving();
  window.addEventListener('scroll', handleScroll, { passive: true });
}

// Отключение функций
function disableFeatures() {
  showAllRows();
  stopObserving();
  window.removeEventListener('scroll', handleScroll);
}

// MutationObserver
let observer = null;

function startObserving() {
  if (observer) {
    return;
  }
  
  observer = new MutationObserver((mutations) => {
    let shouldProcess = false;
    
    mutations.forEach((mutation) => {
      if (mutation.addedNodes.length > 0) {
        mutation.addedNodes.forEach((node) => {
          if (node.nodeType === 1) {
            if (
              (node.matches && node.matches('tr[data-testid="resource-table-row"]')) ||
              (node.querySelector && node.querySelector('tr[data-testid="resource-table-row"]'))
            ) {
              shouldProcess = true;
            }
          }
        });
      }
    });
    
    if (shouldProcess) {
      debounceProcess();
    }
  });
  
  observer.observe(document.body, {
    childList: true,
    subtree: true
  });
}

function stopObserving() {
  if (observer) {
    observer.disconnect();
    observer = null;
  }
}

// Debounce
let debounceTimer = null;

function debounceProcess() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    processRows();
  }, 300);
}

// Обработчик прокрутки
let scrollTimer = null;
let isScrolling = false;

function handleScroll() {
  if (isScrolling) {
    return;
  }
  
  isScrolling = true;
  
  clearTimeout(scrollTimer);
  scrollTimer = setTimeout(() => {
    processRows();
    isScrolling = false;
  }, 500);
}

// Уведомление об обновлении статистики
function notifyStatsUpdate() {
  try {
    chrome.runtime.sendMessage({
      action: 'statsUpdated',
      hiddenNoSubject: document.querySelectorAll(`.${HIDDEN_CLASS_NO_SUBJECT}`).length,
      hiddenArchived: document.querySelectorAll(`.${HIDDEN_CLASS_ARCHIVED}`).length,
      totalTickets: getTotalTicketsCount()
    }).catch(() => {});
  } catch (error) {
    // Popup может быть закрыт
  }
}

// Слушатель сообщений от popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'toggleFeature') {
    const { feature, enabled } = message;
    
    if (feature === 'noSubject') {
      hideNoSubjectEnabled = enabled;
    } else if (feature === 'archived') {
      hideArchivedEnabled = enabled;
    }
    
    if (hideNoSubjectEnabled || hideArchivedEnabled) {
      enableFeatures();
    } else {
      disableFeatures();
    }
    
    processRows();
    sendResponse({ success: true });
  }
  
  if (message.action === 'getStats') {
    const hiddenNoSubject = document.querySelectorAll(`.${HIDDEN_CLASS_NO_SUBJECT}`).length;
    const hiddenArchived = document.querySelectorAll(`.${HIDDEN_CLASS_ARCHIVED}`).length;
    const totalTickets = getTotalTicketsCount();
    
    sendResponse({ 
      hiddenNoSubject,
      hiddenArchived,
      totalTickets,
      isTargetPage: isTargetPage(),
      isInitialized
    });
  }
  
  return true;
});

// Обработка изменений в storage (синхронизация между вкладками)
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'local') {
    let stateChanged = false;
    
    if (changes[STORAGE_KEY_NO_SUBJECT]) {
      hideNoSubjectEnabled = changes[STORAGE_KEY_NO_SUBJECT].newValue;
      stateChanged = true;
    }
    
    if (changes[STORAGE_KEY_ARCHIVED]) {
      hideArchivedEnabled = changes[STORAGE_KEY_ARCHIVED].newValue;
      stateChanged = true;
    }
    
    if (stateChanged && isTargetPage() && isInitialized) {
      if (hideNoSubjectEnabled || hideArchivedEnabled) {
        enableFeatures();
      } else {
        disableFeatures();
      }
      processRows();
    }
  }
});

// Запуск инициализации
init();

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    if (!isInitialized && isTargetPage()) {
      init();
    }
  });
}

window.addEventListener('load', () => {
  if (!isInitialized && isTargetPage()) {
    init();
  } else if (isInitialized && (hideNoSubjectEnabled || hideArchivedEnabled)) {
    processRows();
  }
});
