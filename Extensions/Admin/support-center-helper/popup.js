// Ключи для хранения состояний в chrome.storage
const STORAGE_KEY_NO_SUBJECT = 'hideNoSubject';
const STORAGE_KEY_ARCHIVED = 'hideArchived';

// Элементы DOM
const toggleNoSubject = document.getElementById('toggleNoSubject');
const toggleArchived = document.getElementById('toggleArchived');
const statusDiv = document.getElementById('status');
const counterNoSubjectDiv = document.getElementById('counterNoSubject');
const counterArchivedDiv = document.getElementById('counterArchived');
const totalTicketsDiv = document.getElementById('totalTickets');
const versionDiv = document.querySelector('.version');

// Интервал для обновления счётчиков
let updateInterval = null;

// Загрузка версии из manifest
function loadVersion() {
  const manifestData = chrome.runtime.getManifest();
  versionDiv.textContent = `v${manifestData.version}`;
}

// Загрузка сохраненных состояний при открытии popup
async function loadState() {
  try {
    // Загружаем версию
    loadVersion();
    
    // Загружаем состояния из local storage (одинаковые для всех вкладок)
    const result = await chrome.storage.local.get([
      STORAGE_KEY_NO_SUBJECT,
      STORAGE_KEY_ARCHIVED
    ]);
    
    const hideNoSubject = result[STORAGE_KEY_NO_SUBJECT] || false;
    const hideArchived = result[STORAGE_KEY_ARCHIVED] || false;
    
    toggleNoSubject.checked = hideNoSubject;
    toggleArchived.checked = hideArchived;
    
    // Показываем текущий статус
    await updateCounters();
    
    // Запускаем автоматическое обновление, если хотя бы одна функция включена
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
          // Обновляем общий счётчик
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
  }, 2000);
}

// Остановка автоматического обновления
function stopAutoUpdate() {
  if (updateInterval) {
    clearInterval(updateInterval);
    updateInterval = null;
  }
}

// Сохранение состояния и отправка сообщения в content script
async function saveState(feature, isEnabled) {
  try {
    const storageKey = feature === 'noSubject' 
      ? STORAGE_KEY_NO_SUBJECT
      : STORAGE_KEY_ARCHIVED;
    
    // Сохраняем в local storage (общее для всех вкладок)
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
        
        // Убрали уведомления при переключении тогглов
        
        // Управляем автообновлением
        const otherToggle = feature === 'noSubject' ? toggleArchived : toggleNoSubject;
        if (isEnabled || otherToggle.checked) {
          startAutoUpdate();
          setTimeout(updateCounters, 500);
        } else {
          stopAutoUpdate();
        }
      } catch (error) {
        console.error('Support Center Helper: Content script не отвечает:', error);
        
        if (isEnabled || (feature === 'noSubject' ? toggleArchived.checked : toggleNoSubject.checked)) {
          startAutoUpdate();
        } else {
          stopAutoUpdate();
        }
      }
    } else {
      // Управляем автообновлением
      const otherToggle = feature === 'noSubject' ? toggleArchived : toggleNoSubject;
      if (isEnabled || otherToggle.checked) {
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

// Слушаем изменения в storage (для синхронизации между вкладками)
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'local') {
    if (changes[STORAGE_KEY_NO_SUBJECT]) {
      toggleNoSubject.checked = changes[STORAGE_KEY_NO_SUBJECT].newValue;
    }
    if (changes[STORAGE_KEY_ARCHIVED]) {
      toggleArchived.checked = changes[STORAGE_KEY_ARCHIVED].newValue;
    }
    
    // Управляем автообновлением
    if (toggleNoSubject.checked || toggleArchived.checked) {
      startAutoUpdate();
    } else {
      stopAutoUpdate();
    }
  }
});

// Слушаем обновления от content script
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'statsUpdated') {
    updateCounters();
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

// Инициализация
loadState();
