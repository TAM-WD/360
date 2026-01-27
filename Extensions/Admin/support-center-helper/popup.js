const STORAGE_KEY = 'featureEnabled';

const toggle = document.getElementById('featureToggle');
const statusDiv = document.getElementById('status');
const counterDiv = document.getElementById('counter');

let updateInterval = null;

async function loadState() {
  try {
    const result = await chrome.storage.sync.get(STORAGE_KEY);
    const isEnabled = result[STORAGE_KEY] || false;
    toggle.checked = isEnabled;
    
    await updateCounters();
    
    if (isEnabled) {
      startAutoUpdate();
    }
  } catch (error) {
    console.error('Ошибка загрузки состояния:', error);
  }
}

async function updateCounters() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    if (!tab) {
      return;
    }
    
    if (!tab.url.includes('admin.yandex.ru') && !tab.url.includes('admin.360.yandex.ru')) {
      return;
    }
    
    if (!tab.url.includes('/support-center')) {
      return;
    }
    
    try {
      const response = await chrome.tabs.sendMessage(tab.id, {
        action: 'getStats'
      });
      
      if (response) {
        const { hiddenCount, isTargetPage, isInitialized } = response;
        
        if (!isTargetPage) {
          setCounterText('Не на странице Support Center', 'inactive');
        } else if (!isInitialized) {
          setCounterText('Загрузка...', 'loading');
        } else {
          setCounterText(`Скрыто: ${hiddenCount}`, hiddenCount > 0 ? 'active' : 'zero');
        }
      } else {
      }
    } catch (error) {
      // Content script может быть не загружен
      console.log('Content script не отвечает:', error.message);
      setCounterText('Обновите страницу', 'inactive');
    }
  } catch (error) {
    console.error('Ошибка обновления счётчика:', error);
    setCounterText('Ошибка получения данных', 'error');
  }
}

function setCounterText(text, state = 'default') {
  if (!counterDiv) return;
  
  counterDiv.textContent = text;
  counterDiv.className = 'counter';
  
  switch (state) {
    case 'active':
      counterDiv.classList.add('counter-active');
      break;
    case 'zero':
      counterDiv.classList.add('counter-zero');
      break;
    case 'inactive':
      counterDiv.classList.add('counter-inactive');
      break;
    case 'loading':
      counterDiv.classList.add('counter-loading');
      break;
    case 'error':
      counterDiv.classList.add('counter-error');
      break;
  }
}

function startAutoUpdate() {
  stopAutoUpdate();
  
  updateInterval = setInterval(() => {
    updateCounters();
  }, 2000);
}

function stopAutoUpdate() {
  if (updateInterval) {
    clearInterval(updateInterval);
    updateInterval = null;
  }
}

async function saveState(isEnabled) {
  try {
    await chrome.storage.sync.set({ [STORAGE_KEY]: isEnabled });
    
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    if (tab.url.includes('admin.yandex.ru') || tab.url.includes('admin.360.yandex.ru')) {
      
      try {
        await chrome.tabs.sendMessage(tab.id, {
          action: 'toggleFeature',
          enabled: isEnabled
        });
        
        showStatus(
          isEnabled ? 'Скрытие включено' : 'Скрытие отключено', 
          'success'
        );
        
        if (isEnabled) {
          startAutoUpdate();
          setTimeout(updateCounters, 500);
        } else {
          stopAutoUpdate();
          setCounterText('Функция отключена', 'inactive');
        }
      } catch (error) {
        console.error('Content script не отвечает:', error);
        showStatus('Обновите страницу', 'error');
        
        if (isEnabled) {
          startAutoUpdate();
        } else {
          stopAutoUpdate();
        }
      }
    } else {
      stopAutoUpdate();
    }
  } catch (error) {
    console.error('Ошибка сохранения состояния:', error);
    showStatus('Ошибка сохранения', 'error');
  }
}

function showStatus(message, type) {
  statusDiv.textContent = message;
  statusDiv.className = `status ${type}`;
  
  setTimeout(() => {
    statusDiv.classList.add('hidden');
  }, 3000);
}

toggle.addEventListener('change', (e) => {
  saveState(e.target.checked);
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'sync' && changes[STORAGE_KEY]) {
    const newValue = changes[STORAGE_KEY].newValue;
    toggle.checked = newValue;
    
    if (newValue) {
      startAutoUpdate();
    } else {
      stopAutoUpdate();
      setCounterText('Функция отключена', 'inactive');
    }
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'statsUpdated') {
    updateCounters();
  }
});

window.addEventListener('beforeunload', () => {
  stopAutoUpdate();
});

document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    stopAutoUpdate();
  } else if (toggle.checked) {
    startAutoUpdate();
    updateCounters();
  }
});


loadState();
