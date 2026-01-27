// content.js - независимый скрипт для выполнения экспорта

console.log('Audit Logs Exporter content script loaded');

let isExporting = false;
let isCancelled = false;

// Слушаем сообщения от popup и background
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  console.log('Content script received message:', message);
  
  if (message.action === 'startExport') {
    if (!isExporting) {
      startExport();
      sendResponse({ status: 'started' });
    } else {
      sendResponse({ status: 'already_running' });
    }
  } else if (message.action === 'cancel') {
    isCancelled = true;
    sendResponse({ status: 'cancelling' });
  }
  
  return true;
});

// Основная функция экспорта
async function startExport() {
  try {
    isExporting = true;
    isCancelled = false;
    
    console.log('Starting export...');
    
    // Функция для получения количества строк
    function getRowCount() {
      return document.querySelectorAll('tr[data-testid="resource-table-row"]').length;
    }
    
    // Функция для получения прокручиваемого контейнера
    function getScrollableContainer() {
      const allDivs = document.querySelectorAll('div');
      let maxScrollHeight = 0;
      let scrollableElement = null;
      
      for (const div of allDivs) {
        const style = window.getComputedStyle(div);
        const hasScroll = style.overflow === 'auto' || 
                         style.overflow === 'scroll' || 
                         style.overflowY === 'auto' || 
                         style.overflowY === 'scroll';
        
        if (hasScroll && div.scrollHeight > div.clientHeight) {
          if (div.scrollHeight > maxScrollHeight) {
            maxScrollHeight = div.scrollHeight;
            scrollableElement = div;
          }
        }
      }
      
      return scrollableElement || document.documentElement;
    }
    
    // Быстрая прокрутка
    async function scrollDownFast(element) {
      const isWindow = element === document.documentElement;
      const viewportHeight = isWindow ? window.innerHeight : element.clientHeight;
      
      if (isWindow) {
        window.scrollBy({ top: viewportHeight, behavior: 'auto' });
      } else {
        element.scrollTop += viewportHeight;
      }
      
      await new Promise(resolve => setTimeout(resolve, 100));
      
      if (isWindow) {
        window.scrollTo(0, document.body.scrollHeight);
      } else {
        element.scrollTop = element.scrollHeight;
      }
    }
    
    // Отправка статуса в background
    function sendStatus(status, data) {
      chrome.runtime.sendMessage({
        action: 'updateStatus',
        status: status,
        data: data
      }).catch(err => console.log('Failed to send status:', err));
    }
    
    // Прокрутка страницы
    sendStatus('scrolling', { rows: 0, scrolls: 0 });
    
    const scrollElement = getScrollableContainer();
    const isWindow = scrollElement === document.documentElement;
    
    // ВАЖНО: Сначала прокручиваем в самый верх
    console.log('Scrolling to top of container...');
    if (isWindow) {
      window.scrollTo({ top: 0, behavior: 'auto' });
    } else {
      scrollElement.scrollTop = 0;
    }
    
    // Ждем чтобы страница успела прокрутиться и загрузить контент сверху
    await new Promise(resolve => setTimeout(resolve, 500));
    
    let previousRowCount = 0;
    let currentRowCount = getRowCount();
    let scrollAttempts = 0;
    const maxScrollAttempts = 500;
    let noNewContentAttempts = 0;
    const maxNoNewContentAttempts = 4;
    
    console.log(`Starting scroll. Initial rows: ${currentRowCount}`);
    
    while (scrollAttempts < maxScrollAttempts && 
           noNewContentAttempts < maxNoNewContentAttempts && 
           !isCancelled) {
      previousRowCount = currentRowCount;
      
      await scrollDownFast(scrollElement);
      scrollAttempts++;
      
      await new Promise(resolve => setTimeout(resolve, 500));
      
      currentRowCount = getRowCount();
      sendStatus('scrolling', { rows: currentRowCount, scrolls: scrollAttempts });
      
      if (currentRowCount > previousRowCount) {
        noNewContentAttempts = 0;
        console.log(`Scroll #${scrollAttempts}: ${previousRowCount} → ${currentRowCount}`);
      } else {
        noNewContentAttempts++;
        
        if (noNewContentAttempts < maxNoNewContentAttempts) {
          await new Promise(resolve => setTimeout(resolve, 1000));
          currentRowCount = getRowCount();
          
          if (currentRowCount > previousRowCount) {
            noNewContentAttempts = 0;
          }
        }
      }
    }
    
    if (isCancelled) {
      console.log('Export cancelled');
      isExporting = false;
      return;
    }
    
    console.log(`Scroll complete. Total: ${currentRowCount} rows`);
    sendStatus('collecting', { rows: currentRowCount, scrolls: scrollAttempts });
    
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Сбор данных
    const rows = document.querySelectorAll('tr[data-testid="resource-table-row"]');
    console.log(`Collecting data from ${rows.length} rows...`);
    
    const records = [];
    
    rows.forEach((row, index) => {
      if (isCancelled) return;
      
      try {
        const cells = row.querySelectorAll('td[data-testid="resource-table-column"]');
        if (cells.length < 4) return;
        
        const employeeCell = cells[1];
        const employeeNameElement = employeeCell.querySelector('.Text_color_primary');
        const employeeEmailElement = employeeCell.querySelector('.Text_color_secondary');
        
        const employeeName = employeeNameElement ? employeeNameElement.textContent.trim() : '';
        const employeeEmail = employeeEmailElement ? employeeEmailElement.textContent.trim() : '';
        
        const dateCell = cells[2];
        const dateText = dateCell.textContent.trim();
        
        const eventCell = cells[3];
        const eventText = eventCell.textContent.trim();
        
        if (employeeName || employeeEmail || dateText || eventText) {
          records.push({
            employee: employeeName,
            email: employeeEmail,
            date: dateText,
            event: eventText
          });
        }
      } catch (error) {
        console.error(`Error processing row ${index}:`, error);
      }
    });
    
    if (isCancelled) {
      console.log('Export cancelled during collection');
      isExporting = false;
      return;
    }
    
    console.log(`Collection complete. Total: ${records.length} records`);
    sendStatus('creating', { rows: records.length, scrolls: scrollAttempts });
    
    // Отправляем результат в background
    chrome.runtime.sendMessage({
      action: 'exportComplete',
      result: {
        success: true,
        recordCount: records.length,
        scrollCount: scrollAttempts,
        data: records
      }
    });
    
    console.log('Export completed successfully');
    
  } catch (error) {
    console.error('Export error:', error);
    chrome.runtime.sendMessage({
      action: 'exportComplete',
      result: {
        success: false,
        error: error.message
      }
    });
  } finally {
    isExporting = false;
  }
}
