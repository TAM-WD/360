const STORAGE_KEY = 'featureEnabled';
let isFeatureEnabled = false;
const HIDDEN_CLASS = 'ext-hidden-row';
let isInitialized = false;

function isTargetPage() {
  const path = window.location.pathname;
  return path.includes('/support-center');
}

async function init() {
  if (isInitialized) {
    return;
  }
  if (!isTargetPage()) {
    setupUrlWatcher();
    return;
  }
  
  console.log('Support Center Helper: инициализация на целевой странице');
  isInitialized = true;

  injectStyles();
  
  const result = await chrome.storage.sync.get(STORAGE_KEY);
  isFeatureEnabled = result[STORAGE_KEY] || false;
  
  console.log('Support Center Helper: состояние загружено', isFeatureEnabled);
  
  await waitForPageLoad();
  
  if (isFeatureEnabled) {
    enableFeature();
  }
}

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
  
  // События history API
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
  } else if (isTargetPage() && isInitialized && isFeatureEnabled) {
    setTimeout(() => {
      hideEmptySubjectRows();
    }, 1000);
  }
}

function cleanup() {
  disableFeature();
  isInitialized = false;
}

function injectStyles() {
  if (document.getElementById('ext-hide-styles')) {
    return;
  }
  
  const style = document.createElement('style');
  style.id = 'ext-hide-styles';
  style.textContent = `
    .${HIDDEN_CLASS} {
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

function waitForTable(timeout = 10000) {
  return new Promise((resolve) => {
    const existingRows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
    if (existingRows.length > 0) {
      console.log('Support Center Helper: таблица уже загружена');
      resolve(true);
      return;
    }
    
    //console.log('Support Center Helper: ожидание загрузки таблицы...');
    
    const startTime = Date.now();
    const observer = new MutationObserver((mutations) => {
      const rows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
      
      if (rows.length > 0) {
        console.log('Support Center Helper: таблица появилась');
        observer.disconnect();
        resolve(true);
      } else if (Date.now() - startTime > timeout) {
        //console.log('Support Center Helper: таймаут ожидания таблицы');
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

// Функция для скрытия строк с "Без темы"
function hideEmptySubjectRows() {
  const rows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
  
  if (rows.length === 0) {
    console.log('Support Center Helper: строки не найдены');
    return;
  }
  
  let hiddenCount = 0;
  
  rows.forEach(row => {
    // Пропускаем уже обработанные строки
    if (row.dataset.processedByExt === 'true') {
      return;
    }
    
    // Ищем элементы с текстом внутри строки
    const textElements = row.querySelectorAll('[data-testid="orb-text"]');
    
    textElements.forEach(element => {
      const text = element.textContent.trim();
      
      // Проверяем, содержит ли элемент текст "Без темы"
      if (text === 'Без темы') {
        row.classList.add(HIDDEN_CLASS);
        row.dataset.processedByExt = 'true';
        hiddenCount++;
      }
    });
    
    // Помечаем как обработанную, даже если не скрыли
    if (!row.dataset.processedByExt) {
      row.dataset.processedByExt = 'true';
    }
  });
  
  if (hiddenCount > 0) {
    console.log(`Support Center Helper: скрыто строк "Без темы": ${hiddenCount}`);
    
    // Уведомляем popup об изменении статистики
    notifyStatsUpdate();
  }
}

// Функция для уведомления popup об обновлении статистики
function notifyStatsUpdate() {
  // Отправляем сообщение в runtime (popup может его получить)
  try {
    chrome.runtime.sendMessage({
      action: 'statsUpdated',
      hiddenCount: document.querySelectorAll(`.${HIDDEN_CLASS}`).length
    }).catch(() => {
      // Игнорируем ошибку, если popup не открыт
    });
  } catch (error) {
    // Popup может быть закрыт
  }
}


// Функция для показа всех скрытых строк
function showAllRows() {
  const hiddenRows = document.querySelectorAll(`.${HIDDEN_CLASS}`);
  
  hiddenRows.forEach(row => {
    row.classList.remove(HIDDEN_CLASS);
    row.dataset.processedByExt = 'false';
  });
  
  if (hiddenRows.length > 0) {
    console.log(`Support Center Helper: показано строк: ${hiddenRows.length}`);
    
    // Уведомляем popup об изменении
    notifyStatsUpdate();
  }
}


// Включение функции
async function enableFeature() {
  console.log('Support Center Helper: функция скрытия "Без темы" включена');
  
  // Ждём появления таблицы
  const tableExists = await waitForTable();
  
  if (tableExists) {
    // Скрываем существующие элементы
    hideEmptySubjectRows();
  }
  
  // Наблюдаем за изменениями DOM (для динамической подгрузки)
  startObserving();
  
  // Слушаем события прокрутки (для ленивой загрузки)
  window.addEventListener('scroll', handleScroll, { passive: true });
}

// Отключение функции
function disableFeature() {
  console.log('Support Center Helper: функция скрытия "Без темы" отключена');
  
  // Показываем все скрытые строки
  showAllRows();
  
  // Останавливаем наблюдение
  stopObserving();
  
  // Удаляем слушатель прокрутки
  window.removeEventListener('scroll', handleScroll);
}

// MutationObserver для отслеживания динамических изменений
let observer = null;

function startObserving() {
  // Если observer уже существует, не создаём новый
  if (observer) {
    return;
  }
  
  observer = new MutationObserver((mutations) => {
    // Проверяем, добавились ли новые строки
    let shouldProcess = false;
    
    mutations.forEach((mutation) => {
      if (mutation.addedNodes.length > 0) {
        mutation.addedNodes.forEach((node) => {
          if (node.nodeType === 1) { // Element node
            // Проверяем, является ли добавленный узел строкой таблицы или содержит их
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
      debounceHide();
    }
  });
  
  observer.observe(document.body, {
    childList: true,
    subtree: true
  });
  
  //console.log('Support Center Helper: MutationObserver запущен');
}

function stopObserving() {
  if (observer) {
    observer.disconnect();
    observer = null;
    //console.log('Support Center Helper: MutationObserver остановлен');
  }
}

// Debounce для оптимизации частых вызовов
let debounceTimer = null;

function debounceHide() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    hideEmptySubjectRows();
  }, 300);
}

// Обработчик прокрутки (throttled)
let scrollTimer = null;
let isScrolling = false;

function handleScroll() {
  if (isScrolling) {
    return;
  }
  
  isScrolling = true;
  
  clearTimeout(scrollTimer);
  scrollTimer = setTimeout(() => {
    hideEmptySubjectRows();
    isScrolling = false;
  }, 500);
}

// Слушатель сообщений от popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  console.log('Support Center Helper: получено сообщение', message);
  
  if (message.action === 'toggleFeature') {
    isFeatureEnabled = message.enabled;
    
    if (isFeatureEnabled) {
      enableFeature();
    } else {
      disableFeature();
    }
    
    sendResponse({ success: true });
  }
  
  // Обработчик для получения статистики
  if (message.action === 'getStats') {
    const hiddenCount = document.querySelectorAll(`.${HIDDEN_CLASS}`).length;
    sendResponse({ 
      hiddenCount,
      isTargetPage: isTargetPage(),
      isInitialized
    });
  }
  
  return true; // Важно для асинхронных ответов
});

// Обработка изменений в storage (синхронизация между вкладками)
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'sync' && changes[STORAGE_KEY]) {
    const newValue = changes[STORAGE_KEY].newValue;
    
    console.log('Support Center Helper: состояние изменено в storage', newValue);
    
    if (newValue !== isFeatureEnabled) {
      isFeatureEnabled = newValue;
      
      if (isTargetPage() && isInitialized) {
        if (isFeatureEnabled) {
          enableFeature();
        } else {
          disableFeature();
        }
      }
    }
  }
});

// Запуск инициализации
console.log('Support Center Helper: скрипт загружен', document.readyState);

// Запускаем сразу
init();

// Также пробуем при DOMContentLoaded
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    //console.log('Support Center Helper: DOMContentLoaded');
    if (!isInitialized && isTargetPage()) {
      init();
    }
  });
}

// И при полной загрузке
window.addEventListener('load', () => {
  //console.log('Support Center Helper: window.load');
  if (!isInitialized && isTargetPage()) {
    init();
  } else if (isInitialized && isFeatureEnabled) {
    hideEmptySubjectRows();
  }
});
