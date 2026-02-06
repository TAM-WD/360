const MAPPINGS = {
  status: {
    'WAITING': 'Ждёт уточнения',
    'ARCHIVED': 'Архивирован',
    'CLOSED': 'Решён',
    'IN_PROGRESS': 'В работе',
    'NEW': 'Новый'
  },
  product: {
    'forms': 'Формы',
    'disk': 'Диск',
    'docs': 'Документы',
    'premium': 'Яндекс 360 Премиум',
    'webs': 'Вебинары',
    'wiki': 'Вики',
    'messenger': 'Мессенджер',
    'telemost': 'Телемост',
    'mail': 'Почта',
    'calendar': 'Календарь',
    'yandex_id': 'Яндекс ID',
    'boards': 'Доски',
    'admin': 'Управление организацией',
    'tracker': 'Трекер',
    'sender': 'Рассылки',
    'alicepro': 'Алиса ПРО',
    'notes': 'Заметки',
    'other': 'Другой'
  }
};

function getStatusLabel(status) {
  if (!status) return '';
  const upperStatus = String(status).toUpperCase();
  return MAPPINGS.status[upperStatus] || status;
}

function getProductLabel(product) {
  if (!product) return '';
  const lowerProduct = String(product).toLowerCase();
  return MAPPINGS.product[lowerProduct] || product;
}

function processTicket(ticket) {
  if (!ticket || typeof ticket !== 'object') return ticket;
  
  return {
    ...ticket,
    status: getStatusLabel(ticket.status),
    product: getProductLabel(ticket.product),
    _originalStatus: ticket.status,
    _originalProduct: ticket.product
  };
}

// Хранилище для тикетов (экспорт)
let ticketsData = [];
let exportMetadata = {
  lastUpdate: null,
  filters: null
};

// Хранилище для навигации
let navigationTicketsList = [];
let navigationMetadata = {
  lastUpdate: null,
  filters: null
};

// Обработка сообщений от content script
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  
  // === ЭКСПОРТ ТИКЕТОВ ===
  
  if (message.action === 'addTickets') {
    const newTickets = message.tickets || [];
    const filters = message.filters || {};
    const shouldClear = message.shouldClear || false;
    
    console.log('Background: Получены тикеты для экспорта', {
      newCount: newTickets.length,
      currentCount: ticketsData.length,
      shouldClear,
      filters
    });
    
    // Очищаем список если нужно
    if (shouldClear) {
      console.log('Background: 🗑️ Очистка данных тикетов перед добавлением');
      ticketsData = [];
    }
    
    // Добавляем новые тикеты с обработкой
    let addedCount = 0;
    newTickets.forEach(ticket => {
      const exists = ticketsData.some(t => t.ticket_id === ticket.ticket_id);
      if (!exists) {
        const processedTicket = processTicket(ticket);
        ticketsData.push(processedTicket);
        addedCount++;
      }
    });

    console.log('Background: Добавлено новых тикетов:', addedCount);
    
    // Обновляем метаданные
    exportMetadata = {
      lastUpdate: Date.now(),
      filters: filters
    };
    
    console.log(`Background: Всего тикетов для экспорта: ${ticketsData.length}`);
    
    chrome.runtime.sendMessage({
      action: 'ticketsUpdated',
      count: ticketsData.length
    }).catch(() => {});
    
    sendResponse({ success: true, total: ticketsData.length });
  }
  
  if (message.action === 'getTicketsCount') {
    sendResponse({ count: ticketsData.length });
  }
  
  if (message.action === 'exportTickets') {
    const hideNoSubject = message.hideNoSubject || false;
    const hideArchived = message.hideArchived || false;
    
    console.log('Background: Экспорт тикетов с фильтрацией', {
      total: ticketsData.length,
      hideNoSubject,
      hideArchived
    });
    
    let filteredTickets = ticketsData.filter(ticket => {
      if (hideNoSubject && (!ticket.subject || ticket.subject === 'Без темы')) {
        return false;
      }
      
      if (hideArchived && ticket.status === 'Архивирован') {
        return false;
      }
      
      return true;
    });
    
    console.log('Background: Тикетов после фильтрации:', filteredTickets.length);
    
    const csv = convertToCSV(filteredTickets);
    sendResponse({ csv: csv, count: filteredTickets.length });
  }
  
  // === НАВИГАЦИЯ ===
  
  if (message.action === 'updateNavigationList') {
    const { tickets, filters, shouldClear } = message;
    
    console.log('Background: Получен запрос на обновление списка навигации', {
      newTicketsCount: tickets.length,
      currentListSize: navigationTicketsList.length,
      shouldClear,
      filters
    });
    
    if (shouldClear) {
      console.log('Background: 🗑️ Очистка списка навигации перед добавлением');
      navigationTicketsList = [];
    }
    
    // Добавляем новые тикеты к существующему списку
    tickets.forEach(ticketId => {
      if (!navigationTicketsList.includes(ticketId)) {
        navigationTicketsList.push(ticketId);
      }
    });
    
    // Обновляем метаданные
    navigationMetadata = {
      lastUpdate: Date.now(),
      filters: filters
    };
    
    console.log('Background: ✅ Список навигации обновлён', {
      totalTickets: navigationTicketsList.length,
      list: navigationTicketsList
    });
    
    sendResponse({ 
      success: true, 
      count: navigationTicketsList.length 
    });
  }
  
  if (message.action === 'getNavigationList') {
    console.log('Background: Запрос списка навигации', {
      count: navigationTicketsList.length,
      list: navigationTicketsList
    });
    
    sendResponse({ 
      tickets: navigationTicketsList,
      metadata: navigationMetadata
    });
  }
  
  if (message.action === 'addTicketToNavigation') {
    const { ticketId, position } = message;
    
    if (!navigationTicketsList.includes(ticketId)) {
      if (position === 'start') {
        navigationTicketsList.unshift(ticketId);
      } else if (position === 'end') {
        navigationTicketsList.push(ticketId);
      } else if (typeof position === 'number' && position >= 0) {
        navigationTicketsList.splice(position, 0, ticketId);
      }
      
      console.log('Background: Тикет добавлен в навигацию:', ticketId);
    }
    
    sendResponse({ 
      success: true, 
      list: navigationTicketsList 
    });
  }
  
  if (message.action === 'clearNavigationList') {
    console.log('Background: 🗑️ Очистка списка навигации');
    navigationTicketsList = [];
    navigationMetadata = {
      lastUpdate: null,
      filters: null
    };
    
    sendResponse({ success: true });
  }
  
  if (message.action === 'clearTickets') {
    ticketsData = [];
    exportMetadata = {
      lastUpdate: null,
      filters: null
    };
    
    navigationTicketsList = [];
    navigationMetadata = {
      lastUpdate: null,
      filters: null
    };
    
    console.log('Background: 🗑️ Очищены все данные');
    
    sendResponse({ success: true });
  }
  
  return true;
});

// Конвертация данных в CSV
function convertToCSV(tickets) {
  if (tickets.length === 0) {
    return 'Автор,Дата создания,ID тикета,Внешний ID,Тема,Статус,Сервис\n';
  }
  
  const headers = ['Автор', 'Дата создания', 'ID тикета', 'Внешний ID', 'Тема', 'Статус', 'Сервис'];
  
  function escapeCSV(value) {
    if (value === null || value === undefined) {
      return '';
    }
    
    const stringValue = String(value);
    
    if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
      return `"${stringValue.replace(/"/g, '""')}"`;
    }
    
    return stringValue;
  }
  
  function formatDate(isoString) {
    if (!isoString) return '';
    
    try {
      const date = new Date(isoString);
      
      if (isNaN(date.getTime())) return '';
      
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      
      return `${day}.${month}.${year} ${hours}:${minutes}`;
    } catch (error) {
      return '';
    }
  }
  
  const rows = tickets.map(ticket => {
    const email = ticket.created_by?.email || '';
    
    return [
      escapeCSV(email),
      escapeCSV(formatDate(ticket.created_at)),
      escapeCSV(ticket.ticket_id || ''),
      escapeCSV(ticket.external_ticket_id || ''),
      escapeCSV(ticket.subject || ''),
      escapeCSV(ticket.status || ''),
      escapeCSV(ticket.product || '')
    ].join(',');
  });
  
  const csv = [headers.join(','), ...rows].join('\n');
  
  return '\uFEFF' + csv;
}