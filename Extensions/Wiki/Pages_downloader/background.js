let backupState = {
  isRunning: false,
  currentIndex: 0,
  totalLinks: 0,
  links: [],
  pages: [],
  failed: [],
  tabId: null,
  baseUrl: ''
};

function updateBadge() {
  if (backupState.isRunning) {
    if (backupState.totalLinks > 0) {
      const percent = Math.round((backupState.currentIndex / backupState.totalLinks) * 100);
      chrome.action.setBadgeText({ text: `${percent}%` });
      chrome.action.setBadgeBackgroundColor({ color: '#4299e1' });
    } else {
      chrome.action.setBadgeText({ text: '...' });
      chrome.action.setBadgeBackgroundColor({ color: '#4299e1' });
    }
  } else if (backupState.totalLinks > 0 && backupState.currentIndex === backupState.totalLinks) {
    chrome.action.setBadgeText({ text: '✓' });
    chrome.action.setBadgeBackgroundColor({ color: '#38b2ac' });
  } else {
    chrome.action.setBadgeText({ text: '' });
  }
}

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === 'getState') {
    sendResponse(backupState);
    return true;
  }
  
  if (request.action === 'startBackup') {
    startBackup(request.tabId, request.baseUrl);
    sendResponse({ success: true });
    return true;
  }
  
  if (request.action === 'linksCollected') {
    backupState.links = request.links;
    backupState.totalLinks = request.links.length;
    backupState.isRunning = true;
    
    updateBadge();
    
    processPages();
    sendResponse({ success: true });
    return true;
  }
  
  return true;
});

async function startBackup(tabId, baseUrl) {
  backupState = {
    isRunning: true,
    currentIndex: 0,
    totalLinks: 0,
    links: [],
    pages: [],
    failed: [],
    tabId: tabId,
    baseUrl: baseUrl
  };
  
  updateBadge();
  
  try {
    await chrome.tabs.sendMessage(tabId, { action: 'getLinks' });
  } catch (error) {
    backupState.isRunning = false;
    updateBadge();
  }
}

async function processPages() {
  while (backupState.currentIndex < backupState.totalLinks) {
    const link = backupState.links[backupState.currentIndex];
    const fullUrl = backupState.baseUrl + link;
    
    try {
      await chrome.tabs.update(backupState.tabId, { url: fullUrl });
      await waitForPageLoad(backupState.tabId);
      await sleep(800);
      
      const pageData = await chrome.tabs.sendMessage(backupState.tabId, {
        action: 'extractPageData',
        url: fullUrl
      });
      
      if (pageData.failed) {
        backupState.failed.push({
          href: fullUrl,
          title: pageData.title || link,
          reason: pageData.reason || 'Неизвестная ошибка'
        });
      } else {
        backupState.pages.push(pageData);
      }
      
    } catch (error) {
      backupState.failed.push({
        href: fullUrl,
        title: link,
        reason: error.message
      });
    }
    
    backupState.currentIndex++;
    updateBadge();
  }
  
  backupState.isRunning = false;
  updateBadge();
  
  chrome.notifications.create({
    type: 'basic',
    title: 'Wiki Backup завершен',
    message: `Выгружено: ${backupState.pages.length} | Не выгружено: ${backupState.failed.length}`,
    priority: 2
  });
}

function waitForPageLoad(tabId) {
  return new Promise((resolve) => {
    const listener = (updatedTabId, changeInfo) => {
      if (updatedTabId === tabId && changeInfo.status === 'complete') {
        chrome.tabs.onUpdated.removeListener(listener);
        resolve();
      }
    };
    chrome.tabs.onUpdated.addListener(listener);
    
    setTimeout(() => {
      chrome.tabs.onUpdated.removeListener(listener);
      resolve();
    }, 6000);
  });
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

chrome.runtime.onInstalled.addListener(() => {
  chrome.action.setBadgeText({ text: '' });
});
