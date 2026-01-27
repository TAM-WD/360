let updateInterval = null;

document.addEventListener('DOMContentLoaded', async () => {
  updateStatus();
  updateInterval = setInterval(updateStatus, 1000);
});

window.addEventListener('unload', () => {
  if (updateInterval) {
    clearInterval(updateInterval);
  }
});

async function updateStatus() {
  const state = await chrome.runtime.sendMessage({ action: 'getState' });
  
  const button = document.getElementById('startBackup');
  const downloadButtons = document.getElementById('downloadButtons');
  const downloadFailed = document.getElementById('downloadFailed');
  const statusDiv = document.getElementById('status');
  const progressDiv = document.querySelector('.progress');
  
  if (state.isRunning) {
    button.disabled = true;
    button.innerHTML = '<span class="icon">⏸️</span> Выгрузка выполняется...';
    progressDiv.style.display = 'block';
    downloadButtons.classList.remove('visible');
    
    if (state.totalLinks > 0) {
      updateProgress(state.currentIndex, state.totalLinks);
      statusDiv.className = 'info';
      statusDiv.innerHTML = `
        Обрабатываем страницы:<br>
        <strong>${state.currentIndex} / ${state.totalLinks}</strong>
      `;
    } else {
      statusDiv.className = 'info';
      statusDiv.innerHTML = '🔄 Раскрываем разделы и собираем ссылки...';
    }
  } else if (state.totalLinks > 0 && state.currentIndex === state.totalLinks) {
    button.disabled = false;
    button.innerHTML = '<span class="icon">🚀</span> Начать выгрузку';
    downloadButtons.classList.add('visible');
    progressDiv.style.display = 'block';
    
    if (state.failed.length === 0) {
      downloadFailed.disabled = true;
    } else {
      downloadFailed.disabled = false;
    }
    
    updateProgress(state.totalLinks, state.totalLinks);
    
    statusDiv.className = 'success';
    statusDiv.innerHTML = `
      ✅ <strong>Выгрузка завершена!</strong><br><br>
      📄 Выгружено: <strong>${state.pages.length}</strong><br>
      ⚠️ Не выгружено: <strong>${state.failed.length}</strong>
    `;
  } else {
    button.disabled = false;
    button.innerHTML = '<span class="icon">🚀</span> Начать выгрузку';
  }
}

document.getElementById('startBackup').addEventListener('click', async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  
  if (!tab.url.includes('wiki.yandex.ru')) {
    alert('Откройте страницу Wiki');
    return;
  }
  
  const baseUrl = new URL(tab.url).origin;
  
  await chrome.runtime.sendMessage({
    action: 'startBackup',
    tabId: tab.id,
    baseUrl: baseUrl
  });
  
  updateStatus();
});

document.getElementById('downloadPages').addEventListener('click', async () => {
  const state = await chrome.runtime.sendMessage({ action: 'getState' });
  
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  
  const pagesData = {
    timestamp: new Date().toISOString(),
    baseUrl: state.baseUrl,
    totalPages: state.pages.length,
    pages: state.pages
  };
  
  downloadJSON(pagesData, `wiki-выгруженные-${timestamp}.json`);
});

document.getElementById('downloadFailed').addEventListener('click', async () => {
  const state = await chrome.runtime.sendMessage({ action: 'getState' });
  
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  
  const failedData = {
    timestamp: new Date().toISOString(),
    baseUrl: state.baseUrl,
    totalFailed: state.failed.length,
    failed: state.failed
  };
  
  downloadJSON(failedData, `wiki-не-выгруженные-${timestamp}.json`);
});

function downloadJSON(data, filename) {
  const dataStr = JSON.stringify(data, null, 2);
  const blob = new Blob([dataStr], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  
  chrome.downloads.download({
    url: url,
    filename: filename,
    saveAs: false
  });
}

function updateProgress(current, total) {
  const percent = Math.round((current / total) * 100);
  document.getElementById('progressFill').style.width = percent + '%';
  document.getElementById('progressText').textContent = `${current} / ${total} (${percent}%)`;
}
