let extensionEnabled = true;
let lastHandCount = 0;
let checkTimer = null;
let observer = null;
let statusIndicator = null;
let isInitialized = false;

let lastHandUpSent = 0;
let lastHandDownSent = 0;
const DEBOUNCE_MS = 1;

async function initialize() {
  if (isInitialized) {
    return;
  }
  
  console.log('🎵 [Telemost Sounds] Инициализация расширения...');
  
  const sounds = ['hand-up.mp3', 'hand-down.mp3'];
  for (const sound of sounds) {
    try {
      const url = chrome.runtime.getURL(`sounds/${sound}`);
      const response = await fetch(url);
      if (response.ok) {
        console.log('✅ Звук доступен:', sound);
      }
    } catch (err) {
      console.error('❌ Ошибка:', sound, err);
    }
  }
  
  createStatusIndicator();
  
  setTimeout(() => {
    startObserver();
    console.log('✅ [Telemost Sounds] Расширение запущено!');
  }, 2000);
  
  isInitialized = true;
}

function shutdown() {
  if (!isInitialized) {
    return;
  }
  
  console.log('🔇 [Telemost Sounds] Выключение расширения...');
  
  stopObserver();
  
  if (statusIndicator && statusIndicator.parentNode) {
    statusIndicator.remove();
    statusIndicator = null;
  }
  
  isInitialized = false;
  console.log('⏹️ [Telemost Sounds] Расширение остановлено');
}

function createStatusIndicator() {
  if (statusIndicator) return;
  
  statusIndicator = document.createElement('div');
  statusIndicator.style.cssText = `
    position: fixed;
    bottom: 20px;
    right: 20px;
    padding: 8px 12px;
    background: rgba(76, 175, 80, 0.9);
    color: white;
    border-radius: 20px;
    font-size: 12px;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
    z-index: 10000;
    display: flex;
    align-items: center;
    gap: 6px;
    transition: all 0.3s ease;
    cursor: pointer;
    user-select: none;
  `;
  
  statusIndicator.innerHTML = '🎵 <span>Звуки рук: Включено</span>';
  
  if (document.body) {
    document.body.appendChild(statusIndicator);
  } else {
    document.addEventListener('DOMContentLoaded', () => {
      document.body.appendChild(statusIndicator);
    });
  }
  
  setTimeout(() => {
    if (statusIndicator) {
      statusIndicator.style.opacity = '0';
      statusIndicator.style.pointerEvents = 'none';
    }
  }, 5000);
  
  statusIndicator.addEventListener('mouseenter', () => {
    statusIndicator.style.opacity = '1';
  });
  
  statusIndicator.addEventListener('mouseleave', () => {
    setTimeout(() => {
      if (statusIndicator && !statusIndicator.matches(':hover')) {
        statusIndicator.style.opacity = '0';
        statusIndicator.style.pointerEvents = 'none';
      }
    }, 2000);
  });
}

function updateStatusIndicator(enabled) {
  if (!statusIndicator) {
    if (enabled) {
      createStatusIndicator();
    }
    return;
  }
  
  if (enabled) {
    statusIndicator.innerHTML = '🎵 <span>Звуки рук: Включено</span>';
    statusIndicator.style.background = 'rgba(76, 175, 80, 0.9)';
  } else {
    statusIndicator.innerHTML = '🔇 <span>Звуки рук: Выключено</span>';
    statusIndicator.style.background = 'rgba(158, 158, 158, 0.9)';
  }
  
  statusIndicator.style.opacity = '1';
  statusIndicator.style.pointerEvents = 'auto';
  
  setTimeout(() => {
    if (statusIndicator) {
      statusIndicator.style.opacity = '0';
      statusIndicator.style.pointerEvents = 'none';
    }
  }, 3000);
}

function countRaisedHands() {
  const handImages = document.querySelectorAll('img[src*="76804ce78e8f83e5ca4f4e0d8578fc0a8bcdf887"]');
  const handEmojis = Array.from(document.querySelectorAll('*')).filter(el => {
    return el.textContent === '✋' && el.tagName !== 'SCRIPT';
  });
  const handLabels = document.querySelectorAll('[aria-label*="поднял руку"], [aria-label*="raised hand"]');
  
  return Math.max(handImages.length, handEmojis.length, handLabels.length);
}

function checkHandChanges() {
  const currentCount = countRaisedHands();
  
  if (currentCount !== lastHandCount) {
    console.log('🔄 Изменение количества рук:', lastHandCount, '→', currentCount);
    
    const now = Date.now();
    
    if (currentCount > lastHandCount) {
      const diff = currentCount - lastHandCount;
      
      if (now - lastHandUpSent >= DEBOUNCE_MS) {
        console.log('✋ [Content] Поднято рук:', diff);
        chrome.runtime.sendMessage({action: 'handUp'});
        lastHandUpSent = now;
      }
    } else {
      const diff = lastHandCount - currentCount;
      
      if (now - lastHandDownSent >= DEBOUNCE_MS) {
        console.log('👋 [Content] Опущено рук:', diff);
        chrome.runtime.sendMessage({action: 'handDown'});
        lastHandDownSent = now;
      }
    }
    
    lastHandCount = currentCount;
  }
}

function startObserver() {
  if (checkTimer) {
    return;
  }
  
  console.log('👀 Запуск наблюдателя');
  
  checkTimer = setInterval(checkHandChanges, 500);
  
  observer = new MutationObserver(checkHandChanges);
  observer.observe(document.body, {
    childList: true,
    subtree: true,
    attributes: true,
    attributeFilter: ['src', 'aria-label']
  });
  
  setTimeout(() => {
    lastHandCount = countRaisedHands();
    console.log('📊 Текущее количество рук:', lastHandCount);
  }, 1000);
}

function stopObserver() {
  if (checkTimer) {
    clearInterval(checkTimer);
    checkTimer = null;
  }
  if (observer) {
    observer.disconnect();
    observer = null;
  }
  lastHandCount = 0;
}

chrome.storage.sync.get(['extensionEnabled'], (result) => {
  extensionEnabled = result.extensionEnabled !== false;
  if (extensionEnabled) {
    initialize();
  }
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'sync' && changes.extensionEnabled) {
    const newState = changes.extensionEnabled.newValue;
    extensionEnabled = newState;
    
    if (extensionEnabled) {
      console.log('═══════════════════════════════════════');
      console.log('🔄 [Telemost Sounds] ВКЛЮЧЕНИЕ');
      console.log('═══════════════════════════════════════');
      initialize();
      updateStatusIndicator(true);
    } else {
      console.log('═══════════════════════════════════════');
      console.log('🔄 [Telemost Sounds] ВЫКЛЮЧЕНИЕ');
      console.log('═══════════════════════════════════════');
      updateStatusIndicator(false);
      setTimeout(shutdown, 3000);
    }
  }
});

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  console.log('📨 [Content] Получено:', request.action);
  
  if (request.action === 'ping') {
    sendResponse({pong: true, enabled: extensionEnabled});
    return true;
  }
  
  if (request.action === 'playSound') {
    if (!extensionEnabled) {
      sendResponse({success: false});
      return true;
    }
    
    console.log('🔊 Воспроизведение:', request.sound);
    
    const soundKey = request.sound === 'hand-up.mp3' ? 'customHandUpSound' : 'customHandDownSound';
    
    chrome.storage.local.get([soundKey], (result) => {
      let audioSrc;
      
      if (result[soundKey]) {
        console.log('🎨 Используем кастомный звук');
        audioSrc = result[soundKey];
      } else {
        console.log('🎵 Используем стандартный звук');
        audioSrc = chrome.runtime.getURL(`sounds/${request.sound}`);
      }
      
      const audio = new Audio(audioSrc);
      audio.volume = 0.7;
      
      audio.play()
        .then(() => {
          console.log('✅ Звук воспроизведен');
          sendResponse({success: true});
        })
        .catch(err => {
          console.error('❌ Ошибка:', err);
          sendResponse({success: false});
        });
    });
    
    return true;
  }
  
  if (request.action === 'customSoundUpdated') {
    console.log('🔄 Кастомные звуки обновлены');
    return true;
  }
});

setTimeout(() => {
  chrome.runtime.sendMessage({action: 'contentScriptReady'}).catch(() => {});
}, 500);
