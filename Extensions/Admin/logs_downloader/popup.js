// popup.js - управление интерфейсом расширения

const statusCard = document.getElementById('statusCard');
const statusIcon = document.getElementById('statusIcon');
const statusText = document.getElementById('statusText');
const exportBtn = document.getElementById('exportBtn');
const cancelBtn = document.getElementById('cancelBtn');
const progressBar = document.getElementById('progressBar');
const statsDiv = document.getElementById('stats');
const rowCountEl = document.getElementById('rowCount');
const scrollCountEl = document.getElementById('scrollCount');
const backgroundNotice = document.getElementById('backgroundNotice');
const mainButtons = document.getElementById('mainButtons');
const finalButtons = document.getElementById('finalButtons');
const downloadBtn = document.getElementById('downloadBtn');
const restartBtn = document.getElementById('restartBtn');

let isExporting = false;
let currentTabId = null;
let exportedData = null; // Хранение данных для скачивания

// Функции для управления бейджем
function setBadgeExporting() {
  chrome.action.setBadgeText({ text: '●' });
  chrome.action.setBadgeBackgroundColor({ color: '#ff9800' });
}

function setBadgeComplete() {
  chrome.action.setBadgeText({ text: '✓' });
  chrome.action.setBadgeBackgroundColor({ color: '#4caf50' });
}

function clearBadge() {
  chrome.action.setBadgeText({ text: '' });
}

// Проверяем, находимся ли мы на нужной странице
async function checkPage() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    currentTabId = tab.id;
    
    if (!tab.url) {
      updateStatus('error', '❌', 'Не удалось определить URL страницы');
      return false;
    }
    
    if (!tab.url.startsWith('https://admin.yandex.ru/auditlog-users')) {
      updateStatus('warning', '⚠️', 'Откройте страницу логов в админке');
      exportBtn.disabled = true;
      return false;
    }
    
    // Проверяем сохраненное состояние в chrome.storage
    const storageKey = `export_state_${tab.id}`;
    const result = await chrome.storage.local.get(storageKey);
    const savedState = result[storageKey];
    
    if (savedState) {
      // Восстанавливаем сохраненное состояние
      if (savedState.status === 'completed' && savedState.data) {
        // Экспорт завершен - показываем финальные кнопки
        exportedData = savedState.data;
        updateStatus('success', '✅', `Готово! Собрано ${savedState.recordCount} записей`);
        showStats(true);
        updateStats(savedState.recordCount, savedState.scrollCount || 0);
        showFinalButtons(true);
        
        // Устанавливаем бейдж "готово"
        setBadgeComplete();
        
        return true;
      } else if (savedState.status === 'running') {
        // Экспорт еще идет
        isExporting = true;
        exportBtn.disabled = true;
        exportBtn.textContent = 'Экспорт...';
        cancelBtn.classList.add('visible');
        showProgress(true);
        showStats(true);
        showBackgroundNotice(true);
        updateStats(savedState.rows || 0, savedState.scrolls || 0);
        
        // Показываем понятный статус
        let statusMessage = 'Идет сбор страниц...';
        if (savedState.currentStatus === 'scrolling') {
          statusMessage = `Прокрутка... (${savedState.rows} записей)`;
        } else if (savedState.currentStatus === 'collecting') {
          statusMessage = `Сбор данных... (${savedState.rows} записей)`;
        } else if (savedState.currentStatus === 'creating') {
          statusMessage = `Создание файла... (${savedState.rows} записей)`;
        }
        updateStatus('working', '⚙️', statusMessage);
        
        // Устанавливаем бейдж "в процессе"
        setBadgeExporting();
        
        return true;
      }
    }
    
    // Проверяем состояние экспорта в background service (резервный способ)
    const response = await chrome.runtime.sendMessage({
      action: 'getExportState',
      tabId: tab.id
    });
    
    if (response.state && response.state.status === 'running') {
      isExporting = true;
      exportBtn.disabled = true;
      exportBtn.textContent = 'Экспорт...';
      cancelBtn.classList.add('visible');
      showProgress(true);
      showStats(true);
      showBackgroundNotice(true);
      updateStats(response.state.rows || 0, response.state.scrolls || 0);
      
      // Показываем понятный статус
      let statusMessage = 'Идет сбор страниц...';
      if (response.state.currentStatus === 'scrolling') {
        statusMessage = `Прокрутка... (${response.state.rows} записей)`;
      } else if (response.state.currentStatus === 'collecting') {
        statusMessage = `Сбор данных... (${response.state.rows} записей)`;
      } else if (response.state.currentStatus === 'creating') {
        statusMessage = `Создание файла... (${response.state.rows} записей)`;
      }
      updateStatus('working', '⚙️', statusMessage);
      
      return true;
    }
    
    // Проверяем, есть ли данные в таблице
    const hasData = await checkTableHasData(tab.id);
    
    if (!hasData) {
      updateStatus('warning', '⚠️', 'Настройте фильтры и запустите поиск логов');
      exportBtn.disabled = true;
      return false;
    }
    
    updateStatus('ready', '✅', 'Страница определена');
    exportBtn.disabled = false;
    return true;
    
  } catch (error) {
    updateStatus('error', '❌', 'Ошибка: ' + error.message);
    return false;
  }
}

// Проверяем, есть ли данные в таблице
async function checkTableHasData(tabId) {
  try {
    const results = await chrome.scripting.executeScript({
      target: { tabId: tabId },
      func: function() {
        // Проверяем наличие сообщения "Здесь будет список событий"
        const emptyMessage = document.querySelector('.Text_weight_medium');
        if (emptyMessage && emptyMessage.textContent.includes('Здесь будет список событий')) {
          return false;
        }
        
        // Проверяем наличие строк с данными
        const rows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
        let hasRealData = false;
        
        rows.forEach(row => {
          const cells = row.querySelectorAll('td[data-testid="resource-table-column"]');
          // Если есть больше одной ячейки, значит это не пустая строка
          if (cells.length > 1 && row.querySelector('.Text_color_primary')) {
            hasRealData = true;
          }
        });
        
        return hasRealData;
      }
    });
    
    return results && results[0] && results[0].result;
  } catch (error) {
    console.error('Ошибка проверки таблицы:', error);
    return false;
  }
}

function updateStatus(type, icon, message) {
  statusCard.className = `status-card status-${type}`;
  statusIcon.textContent = icon;
  statusText.textContent = message;
}

function showProgress(show) {
  if (show) {
    progressBar.classList.add('active');
  } else {
    progressBar.classList.remove('active');
  }
}

function showBackgroundNotice(show) {
  if (show) {
    backgroundNotice.classList.add('visible');
  } else {
    backgroundNotice.classList.remove('visible');
  }
}

function showFinalButtons(show) {
  if (show) {
    mainButtons.style.display = 'none';
    finalButtons.classList.add('visible');
  } else {
    mainButtons.style.display = 'flex';
    finalButtons.classList.remove('visible');
  }
}

function showStats(show) {
  statsDiv.style.display = show ? 'flex' : 'none';
}

function updateStats(rows, scrolls) {
  rowCountEl.textContent = rows;
  scrollCountEl.textContent = scrolls;
}

// Обработчик кнопки скачивания
downloadBtn.addEventListener('click', async () => {
  if (exportedData && exportedData.length > 0) {
    try {
      downloadBtn.disabled = true;
      downloadBtn.textContent = '⏳ Скачивание...';
      
      console.log('Manual download triggered with', exportedData.length, 'records');
      
      const success = await createExcelFile(exportedData);
      
      if (success) {
        downloadBtn.textContent = '✅ Скачано!';
        
        setTimeout(() => {
          downloadBtn.disabled = false;
          downloadBtn.textContent = '📥 Скачать';
        }, 2000);
      } else {
        throw new Error('Download failed');
      }
    } catch (error) {
      console.error('Download button error:', error);
      updateStatus('error', '❌', 'Ошибка скачивания: ' + error.message);
      downloadBtn.disabled = false;
      downloadBtn.textContent = '📥 Скачать';
    }
  } else {
    console.error('No data to download');
    updateStatus('error', '❌', 'Нет данных для скачивания');
  }
});

// Обработчик кнопки начать заново
restartBtn.addEventListener('click', async () => {
  exportedData = null;
  
  // Очищаем бейдж
  clearBadge();
  
  // Очищаем сохраненное состояние
  if (currentTabId) {
    const storageKey = `export_state_${currentTabId}`;
    await chrome.storage.local.remove(storageKey);
  }
  
  showFinalButtons(false);
  await checkPage();
});

// Обработчик кнопки отмены
cancelBtn.addEventListener('click', async () => {
  if (isExporting && currentTabId) {
    try {
      // Уведомляем background service об отмене
      chrome.runtime.sendMessage({
        action: 'cancelExport',
        tabId: currentTabId
      });
      
      // Отправляем сообщение в content script для остановки
      await chrome.tabs.sendMessage(currentTabId, { action: 'cancel' });
      updateStatus('warning', '⚠️', 'Экспорт отменен пользователем');
      resetUI();
    } catch (error) {
      console.error('Ошибка отмены:', error);
      resetUI();
    }
  }
});

function resetUI() {
  isExporting = false;
  currentTabId = null;
  exportedData = null;
  exportBtn.disabled = false;
  exportBtn.textContent = 'Начать экспорт';
  cancelBtn.classList.remove('visible');
  showProgress(false);
  showStats(false);
  showBackgroundNotice(false);
  showFinalButtons(false);
}

// Обработчик кнопки экспорта
exportBtn.addEventListener('click', async () => {
  try {
    isExporting = true;
    exportBtn.disabled = true;
    exportBtn.textContent = 'Экспорт...';
    cancelBtn.classList.add('visible');
    
    updateStatus('working', '⚙️', 'Инициализация...');
    showProgress(true);
    showStats(true);
    showBackgroundNotice(true);
    updateStats(0, 0);
    
    // Устанавливаем бейдж "в процессе"
    setBadgeExporting();
    
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    currentTabId = tab.id;
    
    // Уведомляем background service о начале экспорта
    await chrome.runtime.sendMessage({
      action: 'startExport',
      tabId: currentTabId
    });
    
    // Запускаем экспорт через отправку сообщения в content script
    // Content script будет работать независимо от popup
    await chrome.tabs.sendMessage(currentTabId, {
      action: 'startExport'
    });
    
    // Периодически обновляем статус из background service
    const statusUpdateInterval = setInterval(async () => {
      if (!isExporting) {
        clearInterval(statusUpdateInterval);
        return;
      }
      
      try {
        const response = await chrome.runtime.sendMessage({
          action: 'getExportState',
          tabId: currentTabId
        });
        
        if (response.state) {
          if (response.state.status === 'running') {
            updateStats(response.state.rows || 0, response.state.scrolls || 0);
            
            // Обновляем статус в зависимости от текущего этапа
            if (response.state.currentStatus === 'scrolling') {
              updateStatus('working', '🔄', `Прокрутка... (${response.state.rows} записей)`);
            } else if (response.state.currentStatus === 'collecting') {
              updateStatus('working', '📝', `Сбор данных... (${response.state.rows} записей)`);
            } else if (response.state.currentStatus === 'creating') {
              updateStatus('working', '💾', `Создание файла... (${response.state.rows} записей)`);
            } else {
              updateStatus('working', '⚙️', `Экспорт... (${response.state.rows} записей)`);
            }
          } else if (response.state.status === 'completed' && response.state.result) {
            // Экспорт завершен в фоне
            clearInterval(statusUpdateInterval);
            
            // Загружаем результаты
            const storageKey = `export_state_${currentTabId}`;
            const result = await chrome.storage.local.get(storageKey);
            const savedState = result[storageKey];
            
            if (savedState && savedState.data) {
              exportedData = savedState.data;
              
              // Устанавливаем бейдж "готово"
              setBadgeComplete();
              
              // Автоматически скачиваем
              let downloadSuccess = false;
              try {
                console.log('Auto-downloading after background completion...');
                downloadSuccess = await createExcelFile(savedState.data);
              } catch (error) {
                console.error('Auto-download error:', error);
              }
              
              if (downloadSuccess) {
                updateStatus('success', '✅', `Готово! Файл скачан (${savedState.recordCount} записей)`);
              } else {
                updateStatus('success', '✅', `Готово! Собрано ${savedState.recordCount} записей. Нажмите "Скачать"`);
              }
              
              showProgress(false);
              showBackgroundNotice(false);
              showStats(true);
              updateStats(savedState.recordCount, savedState.scrollCount || 0);
              
              isExporting = false;
              cancelBtn.classList.remove('visible');
              showFinalButtons(true);
            }
          }
        }
      } catch (error) {
        console.error('Ошибка обновления статуса:', error);
      }
    }, 1000);
    
    // Content script уже запущен - просто ждем результата через периодические проверки
    // Результат придет через background service
    
  } catch (error) {
    updateStatus('error', '❌', 'Ошибка: ' + error.message);
    resetUI();
  }
});

// Создание CSV файла
async function createExcelFile(records) {
  try {
    console.log('Creating CSV file with', records.length, 'records');
    
    const BOM = '\uFEFF';
    let csvContent = BOM + 'Сотрудник,Email,Дата,Событие\n';
    
    records.forEach(record => {
      const row = [
        `"${(record.employee || '').replace(/"/g, '""')}"`,
        `"${(record.email || '').replace(/"/g, '""')}"`,
        `"${(record.date || '').replace(/"/g, '""')}"`,
        `"${(record.event || '').replace(/"/g, '""')}"`
      ].join(',');
      csvContent += row + '\n';
    });
    
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    
    const filename = `audit_logs_${new Date().toISOString().slice(0, 10)}.csv`;
    
    console.log('Downloading file:', filename);
    
    const downloadId = await chrome.downloads.download({
      url: url,
      filename: filename,
      saveAs: false  // Автоматическое скачивание без диалога
    });
    
    console.log('Download started with ID:', downloadId);
    
    // Очищаем URL после небольшой задержки
    setTimeout(() => {
      URL.revokeObjectURL(url);
      console.log('URL revoked');
    }, 2000);
    
    return true;
    
  } catch (error) {
    console.error('Ошибка создания файла:', error);
    throw error;
  }
}

// Проверяем страницу при открытии popup
checkPage();
