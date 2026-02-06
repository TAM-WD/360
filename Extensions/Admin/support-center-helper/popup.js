// Ключи для хранения состояний в chrome.storage
const STORAGE_KEY_NO_SUBJECT = 'hideNoSubject';
const STORAGE_KEY_ARCHIVED = 'hideArchived';
const STORAGE_KEY_NAVIGATION = 'enableNavigation';

// Элементы DOM
const toggleNoSubject = document.getElementById('toggleNoSubject');
const toggleArchived = document.getElementById('toggleArchived');
const toggleNavigation = document.getElementById('toggleNavigation');
const statusDiv = document.getElementById('status');
const counterNoSubjectDiv = document.getElementById('counterNoSubject');
const counterArchivedDiv = document.getElementById('counterArchived');
const totalTicketsDiv = document.getElementById('totalTickets');
const exportBtn = document.getElementById('exportBtn');
const collectedCountDiv = document.getElementById('collectedCount');
const clearBtn = document.getElementById('clearBtn');

// Интервал для обновления счётчиков
let updateInterval = null;

// Загрузка версии из manifest
function loadVersion() {
  const manifestData = chrome.runtime.getManifest();
  const versionDiv = document.getElementById('version');
  versionDiv.textContent = `v${manifestData.version}`;
}

// Обновление счётчика собранных тикетов
async function updateCollectedCount() {
  try {
    const response = await chrome.runtime.sendMessage({ action: 'getTicketsCount' });
    if (response && response.count !== undefined) {
      collectedCountDiv.textContent = response.count;
      exportBtn.disabled = response.count === 0;
    }
  } catch (error) {
    console.error('Support Center Helper: Ошибка получения количества тикетов:', error);
  }
}

// Загрузка сохраненных состояний при открытии popup
async function loadState() {
  try {
    loadVersion();
    updateCollectedCount();

    const result = await chrome.storage.local.get([
      STORAGE_KEY_NO_SUBJECT,
      STORAGE_KEY_ARCHIVED,
      STORAGE_KEY_NAVIGATION
    ]);
    
    const hideNoSubject = result[STORAGE_KEY_NO_SUBJECT] || false;
    const hideArchived = result[STORAGE_KEY_ARCHIVED] || false;
    const enableNavigation = result[STORAGE_KEY_NAVIGATION] || false;
    
    toggleNoSubject.checked = hideNoSubject;
    toggleArchived.checked = hideArchived;
    toggleNavigation.checked = enableNavigation;
    
    await updateCounters();
    
    if (hideNoSubject || hideArchived) {
      startAutoUpdate();
    }
  } catch (error) {
    console.error('Support Center Helper: Ошибка загрузки состояния:', error);
  }
}

// Получение статистики со страницы
async function updateCounters() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    if (!tab) {
      setCounterText(counterNoSubjectDiv, 'Нет активной вкладки', 'inactive');
      setCounterText(counterArchivedDiv, 'Нет активной вкладки', 'inactive');
      totalTicketsDiv.textContent = '—';
      return;
    }
    
    // Проверяем URL
    if (!tab.url.includes('admin.yandex.ru') && !tab.url.includes('admin.360.yandex.ru')) {
      setCounterText(counterNoSubjectDiv, 'Откройте Центр поддержки', 'inactive');
      setCounterText(counterArchivedDiv, 'Откройте Центр поддержки', 'inactive');
      totalTicketsDiv.textContent = '—';
      return;
    }
    
    if (!tab.url.includes('/support-center')) {
      setCounterText(counterNoSubjectDiv, 'Откройте Центр поддержки', 'inactive');
      setCounterText(counterArchivedDiv, 'Откройте Центр поддержки', 'inactive');
      totalTicketsDiv.textContent = '—';
      return;
    }
    
    // Запрашиваем статистику
    try {
      const response = await chrome.tabs.sendMessage(tab.id, {
        action: 'getStats'
      });
      
      if (response) {
        const { hiddenNoSubject, hiddenArchived, totalTickets, isTargetPage, isInitialized } = response;
        
        if (!isTargetPage) {
          setCounterText(counterNoSubjectDiv, 'Не в Центре поддержки', 'inactive');
          setCounterText(counterArchivedDiv, 'Не в Центре поддержки', 'inactive');
          totalTicketsDiv.textContent = '—';
        } else if (!isInitialized) {
          setCounterText(counterNoSubjectDiv, 'Загрузка...', 'loading');
          setCounterText(counterArchivedDiv, 'Загрузка...', 'loading');
          totalTicketsDiv.textContent = '—';
        } else {
          totalTicketsDiv.textContent = totalTickets || 0;
          
          // Обновляем счётчик "Без темы"
          if (toggleNoSubject.checked) {
            setCounterText(
              counterNoSubjectDiv, 
              `Скрыто: ${hiddenNoSubject}`, 
              hiddenNoSubject > 0 ? 'active' : 'zero'
            );
          } else {
            setCounterText(counterNoSubjectDiv, 'Функция отключена', 'inactive');
          }
          
          // Обновляем счётчик архивированных
          if (toggleArchived.checked) {
            setCounterText(
              counterArchivedDiv, 
              `Скрыто: ${hiddenArchived}`, 
              hiddenArchived > 0 ? 'active' : 'zero'
            );
          } else {
            setCounterText(counterArchivedDiv, 'Функция отключена', 'inactive');
          }
        }
      } else {
        setCounterText(counterNoSubjectDiv, 'Ожидание данных...', 'loading');
        setCounterText(counterArchivedDiv, 'Ожидание данных...', 'loading');
        totalTicketsDiv.textContent = '—';
      }
    } catch (error) {
      setCounterText(counterNoSubjectDiv, 'Обновите страницу', 'inactive');
      setCounterText(counterArchivedDiv, 'Обновите страницу', 'inactive');
      totalTicketsDiv.textContent = '—';
    }
  } catch (error) {
    console.error('Support Center Helper: Ошибка обновления счётчиков:', error);
    setCounterText(counterNoSubjectDiv, 'Ошибка получения данных', 'error');
    setCounterText(counterArchivedDiv, 'Ошибка получения данных', 'error');
    totalTicketsDiv.textContent = '—';
  }
}

// Установка текста и стиля счётчика
function setCounterText(element, text, state = 'default') {
  if (!element) return;
  
  element.textContent = text;
  element.className = 'counter';
  
  switch (state) {
    case 'active':
      element.classList.add('counter-active');
      break;
    case 'zero':
      element.classList.add('counter-zero');
      break;
    case 'inactive':
      element.classList.add('counter-inactive');
      break;
    case 'loading':
      element.classList.add('counter-loading');
      break;
    case 'error':
      element.classList.add('counter-error');
      break;
  }
}

// Запуск автоматического обновления счётчиков
function startAutoUpdate() {
  stopAutoUpdate();
  
  updateInterval = setInterval(() => {
    updateCounters();
    updateCollectedCount();
  }, 2000);
}

// Остановка автоматического обновления
function stopAutoUpdate() {
  if (updateInterval) {
    clearInterval(updateInterval);
    updateInterval = null;
  }
}

// Экспорт тикетов в CSV
async function exportToCSV() {
  try {
    exportBtn.disabled = true;
    
    const originalHTML = exportBtn.innerHTML;
    exportBtn.innerHTML = '<span>Подготовка...</span>';
    
    const countResponse = await chrome.runtime.sendMessage({ action: 'getTicketsCount' });
    
    if (countResponse && countResponse.count > 10000) {
      const confirmed = confirm(`У вас ${countResponse.count} тикетов. Экспорт может занять время. Продолжить?`);
      if (!confirmed) {
        exportBtn.disabled = false;
        exportBtn.innerHTML = originalHTML;
        return;
      }
    }
    
    const hideNoSubject = toggleNoSubject.checked;
    const hideArchived = toggleArchived.checked;
    
    console.log('Popup: Экспорт с фильтрами', { hideNoSubject, hideArchived });
    
    const response = await chrome.runtime.sendMessage({ 
      action: 'exportTickets',
      hideNoSubject: hideNoSubject,
      hideArchived: hideArchived
    });
    
    if (response && response.csv && response.count > 0) {
      const blob = new Blob([response.csv], { type: 'text/csv;charset=utf-8;' });
      
      const now = new Date();
      const dateStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
      const timeStr = `${String(now.getHours()).padStart(2, '0')}-${String(now.getMinutes()).padStart(2, '0')}`;
      const filename = `support_tickets_${dateStr}_${timeStr}.csv`;
      
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      
      setTimeout(() => {
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
      }, 100);
      
    } else if (response && response.count === 0) {
      showStatus('Нет данных для экспорта (все тикеты отфильтрованы)', 'error');
    } else {
      showStatus('Ошибка экспорта', 'error');
    }
  } catch (error) {
    console.error('Support Center Helper: Ошибка экспорта:', error);
    showStatus('Ошибка экспорта', 'error');
  } finally {
    exportBtn.disabled = false;
    exportBtn.innerHTML = `
      <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
        <polyline points="7 10 12 15 17 10"></polyline>
        <line x1="12" y1="15" x2="12" y2="3"></line>
      </svg>
      <span>Скачать тикеты в CSV</span>
    `;
  }
}

// Сохранение состояния и отправка сообщения в content script
async function saveState(feature, isEnabled) {
  try {
    let storageKey;
    
    if (feature === 'noSubject') {
      storageKey = STORAGE_KEY_NO_SUBJECT;
    } else if (feature === 'archived') {
      storageKey = STORAGE_KEY_ARCHIVED;
    } else if (feature === 'navigation') {
      storageKey = STORAGE_KEY_NAVIGATION;
    }
    
    await chrome.storage.local.set({ [storageKey]: isEnabled });
    
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    // Проверяем только если вкладка с центром поддержки
    if ((tab.url.includes('admin.yandex.ru') || tab.url.includes('admin.360.yandex.ru')) &&
        tab.url.includes('/support-center')) {
      try {
        await chrome.tabs.sendMessage(tab.id, {
          action: 'toggleFeature',
          feature: feature,
          enabled: isEnabled
        });
        
        // Управляем автообновлением
        const isNoSubjectActive = feature === 'noSubject' ? isEnabled : toggleNoSubject.checked;
        const isArchivedActive = feature === 'archived' ? isEnabled : toggleArchived.checked;
        if (isNoSubjectActive || isArchivedActive) {
          startAutoUpdate();
          setTimeout(updateCounters, 250);
        } else {
          stopAutoUpdate();
        }
      } catch (error) {
        console.error('Support Center Helper: Content script не отвечает:', error);
        
        if (isEnabled || [toggleNoSubject, toggleArchived].some(t => t.checked)) {
          startAutoUpdate();
        } else {
          stopAutoUpdate();
        }
      }
    } else {
      if (isEnabled || [toggleNoSubject, toggleArchived].some(t => t.checked)) {
        startAutoUpdate();
      } else {
        stopAutoUpdate();
      }
    }
  } catch (error) {
    console.error('Support Center Helper: Ошибка сохранения состояния:', error);
    showStatus('Ошибка сохранения', 'error');
  }
}

// Показ статус-сообщения
function showStatus(message, type) {
  statusDiv.textContent = message;
  statusDiv.className = `status ${type}`;
  
  setTimeout(() => {
    statusDiv.classList.add('hidden');
  }, 3000);
}

// Обработчики изменения тогглов
toggleNoSubject.addEventListener('change', (e) => {
  saveState('noSubject', e.target.checked);
});

toggleArchived.addEventListener('change', (e) => {
  saveState('archived', e.target.checked);
});

toggleNavigation.addEventListener('change', (e) => {
  saveState('navigation', e.target.checked);
});

// Обработчик кнопки экспорта
exportBtn.addEventListener('click', exportToCSV);

// Слушаем изменения в storage
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'local') {
    if (changes[STORAGE_KEY_NO_SUBJECT]) {
      toggleNoSubject.checked = changes[STORAGE_KEY_NO_SUBJECT].newValue;
    }
    if (changes[STORAGE_KEY_ARCHIVED]) {
      toggleArchived.checked = changes[STORAGE_KEY_ARCHIVED].newValue;
    }
    if (changes[STORAGE_KEY_NAVIGATION]) {
      toggleNavigation.checked = changes[STORAGE_KEY_NAVIGATION].newValue;
    }
    
    // Управляем автообновлением
    if (toggleNoSubject.checked || toggleArchived.checked) {
      startAutoUpdate();
    } else {
      stopAutoUpdate();
    }
  }
});

// Слушаем обновления от content script и background
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'statsUpdated') {
    updateCounters();
  }
  
  if (message.action === 'ticketsUpdated') {
    updateCollectedCount();
  }
});

// Очистка при закрытии popup
window.addEventListener('beforeunload', () => {
  stopAutoUpdate();
});

// Останавливаем при потере видимости
document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    stopAutoUpdate();
  } else if (toggleNoSubject.checked || toggleArchived.checked) {
    startAutoUpdate();
    updateCounters();
  }
});

// Очистка собранных тикетов
clearBtn.addEventListener('click', async () => {
  try {
    await chrome.runtime.sendMessage({ action: 'clearTickets' });
    updateCollectedCount();
  } catch (error) {
    console.error('Support Center Helper: Ошибка очистки:', error);
    showStatus('Ошибка очистки', 'error');
  }
});

loadState();