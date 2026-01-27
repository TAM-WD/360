console.log('🚀 [Background] Service Worker запущен');

const readyTabs = new Set();
let extensionEnabled = true;
let soundQueue = [];
let isPlaying = false;

// Дебаунс
let lastHandUpTime = 0;
let lastHandDownTime = 0;
const DEBOUNCE_MS = 1;

chrome.storage.sync.get(['extensionEnabled'], (result) => {
  extensionEnabled = result.extensionEnabled !== false;
  console.log('⚙️ [Background] Начальное состояние:', extensionEnabled ? 'включено' : 'выключено');
});

async function playSound(soundFile, settingKey, senderTabId) {
  if (!extensionEnabled) {
    return;
  }
  
  const settings = await chrome.storage.sync.get([settingKey]);
  if (settings[settingKey] === false) {
    return;
  }
  
  // Используем табId отправителя, если он есть
  let targetTabId = senderTabId;
  
  // Если tabId не передан, берем из readyTabs или ищем
  if (!targetTabId) {
    const readyTabsArray = Array.from(readyTabs);
    if (readyTabsArray.length > 0) {
      targetTabId = readyTabsArray[0];
    } else {
      // Ищем любую вкладку Телемоста
      const tabs = await chrome.tabs.query({});
      const telemostTab = tabs.find(tab => 
        tab.url && (
          tab.url.includes('telemost.yandex.ru') || 
          tab.url.includes('telemost.360.yandex.ru')
        )
      );
      
      if (telemostTab) {
        targetTabId = telemostTab.id;
        readyTabs.add(targetTabId); // Добавляем в readyTabs
      } else {
        console.warn('⚠️ Нет вкладок Телемоста');
        return;
      }
    }
  }
  
  try {
    console.log('🎵 Воспроизводим:', soundFile, 'на вкладке', targetTabId);
    await chrome.tabs.sendMessage(targetTabId, {
      action: 'playSound',
      sound: soundFile
    });
    console.log('✅ Звук воспроизведен');
  } catch (err) {
    console.error('❌ Ошибка:', err.message);
    readyTabs.delete(targetTabId);
  }
}

async function processQueue() {
  if (!extensionEnabled || isPlaying || soundQueue.length === 0) {
    return;
  }
  
  isPlaying = true;
  
  while (soundQueue.length > 0 && extensionEnabled) {
    const item = soundQueue.shift();
    await playSound(item.sound, item.setting, item.tabId);
    
    if (soundQueue.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 300));
    }
  }
  
  isPlaying = false;
}

function enqueueSound(soundFile, settingKey, tabId) {
  if (!extensionEnabled) {
    return;
  }
  
  // Дебаунс
  const now = Date.now();
  if (soundFile === 'hand-up.mp3') {
    if (now - lastHandUpTime < DEBOUNCE_MS) {
      return;
    }
    lastHandUpTime = now;
  } else if (soundFile === 'hand-down.mp3') {
    if (now - lastHandDownTime < DEBOUNCE_MS) {
      return;
    }
    lastHandDownTime = now;
  }
  
  soundQueue.push({
    sound: soundFile,
    setting: settingKey,
    tabId: tabId // Сохраняем tabId отправителя
  });
  
  console.log('📥 Добавлен в очередь:', soundFile);
  processQueue();
}

// Обработка сообщений
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  // При любом сообщении от content script добавляем вкладку в readyTabs
  if (sender.tab?.id) {
    readyTabs.add(sender.tab.id);
  }
  
  if (request.action === 'ping') {
    sendResponse({pong: true, enabled: extensionEnabled});
    return true;
  }
  
  if (!extensionEnabled && request.action !== 'extensionToggled' && request.action !== 'contentScriptReady') {
    sendResponse({status: 'disabled'});
    return true;
  }
  
  if (request.action === 'contentScriptReady') {
    if (sender.tab?.id) {
      readyTabs.add(sender.tab.id);
      console.log('✅ Content script готов на вкладке', sender.tab.id);
    }
    sendResponse({status: 'ok'});
    return true;
  }
  
  if (request.action === 'extensionToggled') {
    extensionEnabled = request.enabled;
    
    console.log('═══════════════════════════════════════');
    console.log('🔄 [Background] Расширение:', extensionEnabled ? 'ВКЛЮЧЕНО' : 'ВЫКЛЮЧЕНО');
    console.log('═══════════════════════════════════════');
    
    if (!extensionEnabled) {
      soundQueue = [];
      isPlaying = false;
      console.log('🔒 [Background] Очередь очищена, расширение остановлено');
    }
    
    sendResponse({status: 'ok'});
    return true;
  }
  
  if (request.action === 'handUp') {
    console.log('✋ [Background] РУКА ПОДНЯТА!');
    enqueueSound('hand-up.mp3', 'handUpEnabled', sender.tab?.id);
    sendResponse({status: 'ok'});
    return true;
  }
  
  if (request.action === 'handDown') {
    console.log('👋 [Background] РУКА ОПУЩЕНА!');
    enqueueSound('hand-down.mp3', 'handDownEnabled', sender.tab?.id);
    sendResponse({status: 'ok'});
    return true;
  }
  
  if (request.action === 'testSound') {
    console.log('🧪 [Background] Тест:', request.sound);
    const setting = request.sound === 'hand-up.mp3' ? 'handUpEnabled' : 'handDownEnabled';
    enqueueSound(request.sound, setting, sender.tab?.id);
    sendResponse({status: 'ok'});
    return true;
  }
  
  return true;
});

chrome.tabs.onRemoved.addListener((tabId) => {
  readyTabs.delete(tabId);
});

console.log('✅ [Background] Расширение инициализировано');
