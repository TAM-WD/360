// MAIN world
(function() {
  console.log('Support Center Helper: Interceptor загружен');
  
  // Отслеживание последовательности запросов
  let lastRequestType = null;
  let lastRequestTime = 0;
  
  // Перехват fetch
  const originalFetch = window.fetch;
  
  window.fetch = async function(...args) {
    const url = typeof args[0] === 'string' ? args[0] : args[0]?.url;
    const options = args[1] || {};
    
    const isTicketsRequest = url && (
      url.includes('support-b2b/get-tickets') ||
      (url.includes('/api/models') && options.body && typeof options.body === 'string' && options.body.includes('support-b2b/get-tickets'))
    );

    const isTicketDetailsRequest = url && (
      url.includes('support-b2b/get-ticket') && !url.includes('get-tickets') ||
      (url.includes('/api/models') && options.body && typeof options.body === 'string' && 
       options.body.includes('support-b2b/get-ticket') && !options.body.includes('support-b2b/get-tickets'))
    );
    
    const response = await originalFetch.apply(this, args);
    
    // Обработка списка тикетов
    if (isTicketsRequest) {
      const clonedResponse = response.clone();
      const currentTime = Date.now();
      
      clonedResponse.json().then(data => {
        console.log('Interceptor: Перехвачен ответ get-tickets', data);
        
        let tickets = [];
        let filters = {};
        
        if (data && data.models && Array.isArray(data.models)) {
          data.models.forEach(model => {
            if (model.name === 'support-b2b/get-tickets' && model.data && model.data.items) {
              tickets = model.data.items;
            }
          });

          // Извлекаем фильтры из запроса
          try {
            const requestBody = JSON.parse(options.body);
            if (requestBody.models && requestBody.models[0] && requestBody.models[0].params) {
              filters = requestBody.models[0].params;
            }
          } catch (e) {
            console.warn('Interceptor: Не удалось извлечь фильтры', e);
          }
        } else if (data && data.data && data.data.items) {
          tickets = data.data.items;
        }
        
        if (tickets.length > 0) {
          console.log(`Interceptor: Найдено ${tickets.length} тикетов`);
          console.log('Interceptor: ID тикетов:', tickets.map(t => t.ticket_id));
          
          // Проверяем последовательность запросов
          const shouldClearList = lastRequestType === 'get-ticket' && 
                                  (currentTime - lastRequestTime) < 2000;
          
          if (shouldClearList) {
            console.log('Interceptor: 🔄 Обнаружен переход из тикета в список, очищаем навигацию');
            window.dispatchEvent(new CustomEvent('shouldClearNavigation', {
              detail: { reason: 'backToList' }
            }));
          }
          
          // Отправляем данные тикетов для экспорта
          window.dispatchEvent(new CustomEvent('ticketsReceived', {
            detail: { tickets: tickets, filters: filters }
          }));

          // Отправляем данные для навигации
          window.dispatchEvent(new CustomEvent('ticketsListReceived', {
            detail: { tickets: tickets, filters: filters }
          }));
          
          // Обновляем последний тип запроса
          lastRequestType = 'get-tickets';
          lastRequestTime = currentTime;
        }
      }).catch(err => {
        console.error('Interceptor: Ошибка парсинга JSON get-tickets:', err);
      });
    }

    // Обработка деталей тикета
    if (isTicketDetailsRequest) {
      const clonedResponse = response.clone();
      const currentTime = Date.now();
      
      clonedResponse.json().then(data => {
        console.log('Interceptor: Перехвачен ответ get-ticket', data);

        // Извлекаем данные из запроса
        try {
          const requestBody = JSON.parse(options.body);
          const headers = options.headers || {};
          
          let orgId = null;
          let ticketId = null;
          let sarahCkey = null;
          let uid = null;

          if (requestBody.models && requestBody.models[0] && requestBody.models[0].params) {
            orgId = requestBody.models[0].params.org_id;
            ticketId = requestBody.models[0].params.ticket_id;
          }

          // Получаем заголовки
          if (headers instanceof Headers) {
            sarahCkey = headers.get('x-sarah-ckey');
            uid = headers.get('x-sarah-uid');
          } else if (typeof headers === 'object') {
            sarahCkey = headers['x-sarah-ckey'];
            uid = headers['x-sarah-uid'];
          }

          console.log('Interceptor: Извлечённые данные запроса:', {
            orgId,
            ticketId,
            hasSarahCkey: !!sarahCkey,
            uid
          });

          if (orgId && ticketId) {
            // Отправляем данные для навигации
            window.dispatchEvent(new CustomEvent('ticketDetailsReceived', {
              detail: { orgId, ticketId, sarahCkey, uid }
            }));
          }
          
          // Обновляем последний тип запроса
          lastRequestType = 'get-ticket';
          lastRequestTime = currentTime;
          
        } catch (e) {
          console.error('Interceptor: Ошибка извлечения данных из запроса get-ticket:', e);
        }
      }).catch(err => {
        console.error('Interceptor: Ошибка парсинга JSON get-ticket:', err);
      });
    }
    
    return response;
  };
  
  // Перехват XHR
  const originalOpen = XMLHttpRequest.prototype.open;
  const originalSend = XMLHttpRequest.prototype.send;
  const originalSetRequestHeader = XMLHttpRequest.prototype.setRequestHeader;
  
  XMLHttpRequest.prototype.open = function(method, url, ...rest) {
    this._url = url;
    this._method = method;
    return originalOpen.apply(this, [method, url, ...rest]);
  };

  XMLHttpRequest.prototype.setRequestHeader = function(header, value) {
    if (!this._requestHeaders) {
      this._requestHeaders = {};
    }
    this._requestHeaders[header] = value;
    return originalSetRequestHeader.apply(this, arguments);
  };
  
  XMLHttpRequest.prototype.send = function(body) {
    this._body = body;
    
    const isTicketsRequest = this._url && (
      this._url.includes('support-b2b/get-tickets') ||
      (this._url.includes('/api/models') && body && typeof body === 'string' && body.includes('support-b2b/get-tickets'))
    );

    const isTicketDetailsRequest = this._url && (
      this._url.includes('support-b2b/get-ticket') && !this._url.includes('get-tickets') ||
      (this._url.includes('/api/models') && body && typeof body === 'string' && 
       body.includes('support-b2b/get-ticket') && !body.includes('support-b2b/get-tickets'))
    );
    
    if (isTicketsRequest || isTicketDetailsRequest) {
      this.addEventListener('load', function() {
        try {
          const data = JSON.parse(this.responseText);
          const currentTime = Date.now();
          
          if (isTicketsRequest) {
            console.log('Interceptor XHR: Перехвачен ответ get-tickets', data);
            
            let tickets = [];
            let filters = {};
            
            if (data && data.models && Array.isArray(data.models)) {
              data.models.forEach(model => {
                if (model.name === 'support-b2b/get-tickets' && model.data && model.data.items) {
                  tickets = model.data.items;
                }
              });

              try {
                const requestBody = JSON.parse(body);
                if (requestBody.models && requestBody.models[0] && requestBody.models[0].params) {
                  filters = requestBody.models[0].params;
                }
              } catch (e) {
                console.warn('Interceptor XHR: Не удалось извлечь фильтры', e);
              }
            } else if (data && data.data && data.data.items) {
              tickets = data.data.items;
            }
            
            if (tickets.length > 0) {
              console.log(`Interceptor XHR: Найдено ${tickets.length} тикетов`);
              
              // Проверяем последовательность запросов
              const shouldClearList = lastRequestType === 'get-ticket' && 
                                      (currentTime - lastRequestTime) < 2000;
              
              if (shouldClearList) {
                console.log('Interceptor XHR: 🔄 Обнаружен переход из тикета в список, очищаем навигацию');
                window.dispatchEvent(new CustomEvent('shouldClearNavigation', {
                  detail: { reason: 'backToList' }
                }));
              }
              
              window.dispatchEvent(new CustomEvent('ticketsReceived', {
                detail: { tickets: tickets, filters: filters }
              }));

              window.dispatchEvent(new CustomEvent('ticketsListReceived', {
                detail: { tickets: tickets, filters: filters }
              }));
              
              lastRequestType = 'get-tickets';
              lastRequestTime = currentTime;
            }
          }

          if (isTicketDetailsRequest) {
            console.log('Interceptor XHR: Перехвачен ответ get-ticket', data);
            
            try {
              const requestBody = JSON.parse(body);
              let orgId = null;
              let ticketId = null;

              if (requestBody.models && requestBody.models[0] && requestBody.models[0].params) {
                orgId = requestBody.models[0].params.org_id;
                ticketId = requestBody.models[0].params.ticket_id;
              }

              // Получаем заголовки из сохранённых
              const sarahCkey = this._requestHeaders?.['x-sarah-ckey'];
              const uid = this._requestHeaders?.['x-sarah-uid'];

              console.log('Interceptor XHR: Извлечённые данные запроса:', {
                orgId,
                ticketId,
                hasSarahCkey: !!sarahCkey,
                uid
              });

              if (orgId && ticketId) {
                window.dispatchEvent(new CustomEvent('ticketDetailsReceived', {
                  detail: { orgId, ticketId, sarahCkey, uid }
                }));
              }
              
              lastRequestType = 'get-ticket';
              lastRequestTime = currentTime;
              
            } catch (e) {
              console.error('Interceptor XHR: Ошибка извлечения данных:', e);
            }
          }
        } catch (error) {
          console.error('Interceptor XHR: Ошибка парсинга:', error);
        }
      });
    }
    
    return originalSend.apply(this, arguments);
  };
})();