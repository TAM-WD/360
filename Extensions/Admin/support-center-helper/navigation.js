// Модуль навигации между тикетами с надёжным хранением
class TicketNavigation {
  constructor() {
    this.currentTicketId = null;
    this.orgId = null;
    this.sarahCkey = null;
    this.uid = null;
    this.isInitialized = false;
    this.injectionAttempts = 0;
    this.maxInjectionAttempts = 10;
    this.isEnabled = false;
    this.hideNoSubject = false;
    this.hideArchived = false;
    this.lastFilters = null;
    this.lastUrl = null;
  }


  // Инициализация навигации
  async init() {
    if (this.isInitialized) {
      console.log('🎯 Ticket Navigation: Уже инициализирована');
      return;
    }
    
    console.log('🎯 Ticket Navigation: Начало инициализации');
    
    // Сохраняем текущий URL
    this.lastUrl = window.location.href;
    
    // Слушаем событие очистки навигации
    window.addEventListener('shouldClearNavigation', async (event) => {
      console.log('🗑️ Ticket Navigation: Получен сигнал очистки списка', event.detail);
      await this.clearNavigationList();
    });
    
    // Слушаем изменение настроек
    window.addEventListener('navigationSettingChanged', (event) => {
      const { enabled, hideNoSubject, hideArchived } = event.detail;
      console.log('⚙️ Ticket Navigation: Настройки изменились', event.detail);
      
      this.isEnabled = enabled;
      this.hideNoSubject = hideNoSubject;
      this.hideArchived = hideArchived;
      
      if (enabled) {
        console.log('✅ Ticket Navigation: Функция включена');
        if (this.isTicketViewPage()) {
          this.waitAndInjectButtons();
        }
      } else {
        console.log('❌ Ticket Navigation: Функция отключена');
        this.removeNavigationButtons();
      }
    });

    // Слушаем изменение фильтров
    window.addEventListener('navigationFiltersChanged', async (event) => {
      const { hideNoSubject, hideArchived } = event.detail;
      console.log('🔄 Ticket Navigation: Фильтры изменились', event.detail);
      
      this.hideNoSubject = hideNoSubject;
      this.hideArchived = hideArchived;
      
      if (this.isEnabled) {
        await this.updateButtonsState();
      }
    });
    
    // Слушаем события от interceptor
    window.addEventListener('ticketsListReceived', async (event) => {
      if (!this.isEnabled) {
        console.log('⏸️ Ticket Navigation: Функция отключена, пропускаем обновление списка');
        return;
      }
      
      console.log('📋 Ticket Navigation: Получен список тикетов', event.detail);
      await this.handleTicketsListUpdate(event.detail);
    });

    window.addEventListener('ticketDetailsReceived', async (event) => {
      if (!this.isEnabled) {
        return;
      }
      
      console.log('🎫 Ticket Navigation: Получены детали тикета', event.detail);
      await this.handleTicketDetailsUpdate(event.detail);
    });

    // Проверяем текущую страницу
    if (this.isTicketViewPage()) {
      this.currentTicketId = this.getTicketIdFromUrl();
      console.log('📍 Ticket Navigation: Текущий тикет:', this.currentTicketId);
    }

    // Отслеживаем изменения URL
    this.setupUrlWatcher();
    
    this.isInitialized = true;
    console.log('✅ Ticket Navigation: Инициализация завершена');
  }


  // Проверка, находимся ли на странице просмотра тикета
  isTicketViewPage() {
    const path = window.location.pathname;
    return path.includes('/support-center/') && path.split('/').filter(p => p).length >= 2;
  }


  // Проверка, находимся ли на странице списка тикетов
  isTicketListPage() {
    const path = window.location.pathname;
    return (path === '/support-center' || path === '/support-center/') && window.location.search;
  }


  // Получить ticket_id из URL
  getTicketIdFromUrl() {
    const match = window.location.pathname.match(/\/support-center\/([^/?]+)/);
    return match ? match[1] : null;
  }


  // Получить UID из URL
  getUidFromUrl() {
    const params = new URLSearchParams(window.location.search);
    return params.get('uid');
  }


  // Проверка, изменились ли фильтры
  hasFiltersChanged(newFilters) {
    if (!this.lastFilters) {
      console.log('📋 Ticket Navigation: Первая загрузка фильтров');
      return true;
    }
    
    const keys = ['org_id', 'subject', 'author_uids', 'products', 'statuses', 
                  'created_at_from', 'created_at_to', 'updated_at_from', 'updated_at_to'];
    
    for (const key of keys) {
      const oldValue = JSON.stringify(this.lastFilters[key]);
      const newValue = JSON.stringify(newFilters[key]);
      
      if (oldValue !== newValue) {
        console.log(`🔄 Ticket Navigation: Фильтр "${key}" изменился`, { 
          old: oldValue, 
          new: newValue 
        });
        return true;
      }
    }
    
    return false;
  }


  // Получить список тикетов из background
  async getNavigationList() {
    try {
      return new Promise((resolve) => {
        const timeout = setTimeout(() => {
          console.log('⏱️ Ticket Navigation: Таймаут получения списка');
          resolve([]);
        }, 2000);

        const handler = (event) => {
          clearTimeout(timeout);
          window.removeEventListener('navigationListResponse', handler);
          
          const { tickets = [] } = event.detail;
          console.log('📜 Ticket Navigation: Список получен из background:', {
            count: tickets.length,
            tickets: tickets
          });
          
          resolve(tickets);
        };

        window.addEventListener('navigationListResponse', handler);
        window.dispatchEvent(new CustomEvent('requestNavigationList'));
      });
    } catch (error) {
      console.error('❌ Ticket Navigation: Ошибка получения списка', error);
      return [];
    }
  }


  // Обновить список тикетов в background
  async updateNavigationList(tickets, filters, shouldClear = false) {
    try {
      return new Promise((resolve) => {
        const timeout = setTimeout(() => {
          console.log('⏱️ Ticket Navigation: Таймаут обновления списка');
          resolve(false);
        }, 2000);

        const handler = (event) => {
          clearTimeout(timeout);
          window.removeEventListener('navigationListUpdated', handler);
          
          console.log('✅ Ticket Navigation: Список обновлён в background');
          resolve(true);
        };

        window.addEventListener('navigationListUpdated', handler);
        window.dispatchEvent(new CustomEvent('updateNavigationList', {
          detail: { tickets, filters, shouldClear }
        }));
      });
    } catch (error) {
      console.error('❌ Ticket Navigation: Ошибка обновления списка', error);
      return false;
    }
  }


  // Очистка списка навигации
  async clearNavigationList() {
    console.log('🗑️ Ticket Navigation: Очистка списка навигации');
    
    try {
      return new Promise((resolve) => {
        const timeout = setTimeout(() => {
          console.log('⏱️ Ticket Navigation: Таймаут очистки списка');
          resolve(false);
        }, 2000);

        const handler = () => {
          clearTimeout(timeout);
          window.removeEventListener('navigationListCleared', handler);
          console.log('✅ Ticket Navigation: Список очищен');
          
          // Сбрасываем lastFilters
          this.lastFilters = null;
          
          resolve(true);
        };

        window.addEventListener('navigationListCleared', handler);
        window.dispatchEvent(new CustomEvent('clearNavigationList'));
      });
    } catch (error) {
      console.error('❌ Ticket Navigation: Ошибка очистки списка', error);
      return false;
    }
  }


  // Обработка обновления списка тикетов
  async handleTicketsListUpdate(data) {
    const { tickets, filters } = data;
    
    console.log('📋 Ticket Navigation: Обработка списка тикетов', {
      receivedCount: tickets.length,
      currentFilters: this.lastFilters,
      newFilters: filters,
      hideNoSubject: this.hideNoSubject,
      hideArchived: this.hideArchived
    });
    
    // Проверяем, изменились ли фильтры
    const filtersChanged = this.hasFiltersChanged(filters);
    
    // Фильтруем тикеты согласно настройкам
    let filteredTickets = tickets.filter(ticket => {
      if (this.hideNoSubject && (!ticket.subject || ticket.subject === 'Без темы')) {
        return false;
      }
      
      if (this.hideArchived && ticket.status === 'ARCHIVED') {
        return false;
      }
      
      return true;
    });
    
    // Извлекаем только ticket_id
    const ticketIds = filteredTickets.map(t => t.ticket_id).filter(id => id);
    
    console.log('📋 Ticket Navigation: Отфильтрованный список', {
      original: tickets.length,
      filtered: ticketIds.length,
      filtersChanged,
      ids: ticketIds
    });
    
    // Обновляем список в background
    await this.updateNavigationList(ticketIds, filters, filtersChanged);
    
    // Сохраняем текущие фильтры
    this.lastFilters = filters;
    
    // Обновляем кнопки
    await this.updateButtonsState();
  }


  //Обработка получения деталей тикета
  async handleTicketDetailsUpdate(data) {
    const { orgId, sarahCkey, uid, ticketId } = data;
    
    this.orgId = orgId;
    this.sarahCkey = sarahCkey;
    this.uid = uid;
    this.currentTicketId = ticketId;
    
    console.log('🎫 Ticket Navigation: Данные тикета сохранены', {
      orgId,
      ticketId,
      hasKey: !!sarahCkey,
      uid
    });

    // Обновляем состояние кнопок
    await this.updateButtonsState();
  }


  // Ожидание и внедрение кнопок
  async waitAndInjectButtons() {
    console.log('⏳ Ticket Navigation: Ожидание элементов для внедрения');
    
    const checkAndInject = async () => {
      this.injectionAttempts++;
      
      let backButton = document.querySelector('[data-testid="back-button"]');
      
      if (!backButton) {
        const buttons = Array.from(document.querySelectorAll('button, a'));
        backButton = buttons.find(btn => btn.textContent.includes('Назад'));
      }
      
      if (backButton) {
        console.log('✅ Ticket Navigation: Кнопка "Назад" найдена');
        await this.injectNavigationButtons();
        return true;
      }
      
      if (this.injectionAttempts < this.maxInjectionAttempts) {
        setTimeout(checkAndInject, 500);
      } else {
        console.error('❌ Ticket Navigation: Превышен лимит попыток внедрения');
      }
      
      return false;
    };

    await checkAndInject();
  }


  // Внедрение кнопок навигации
  async injectNavigationButtons() {
    console.log('🔧 Ticket Navigation: Внедрение кнопок');
    
    // Проверяем, не добавлены ли уже кнопки
    if (document.querySelector('.ticket-nav-buttons-wrapper')) {
      console.log('ℹ️ Ticket Navigation: Кнопки уже добавлены, обновляем состояние');
      await this.updateButtonsState();
      return;
    }

  let backButton = document.querySelector('[data-testid="back-button"]');
  
  if (!backButton) {
    const buttons = Array.from(document.querySelectorAll('button, a'));
    backButton = buttons.find(btn => btn.textContent.includes('Назад'));
  }
  
  if (!backButton) {
    console.error('❌ Ticket Navigation: Кнопка "Назад" не найдена');
    return;
  }

  const backButtonContainer = backButton.parentElement;
  const parentContainer = backButtonContainer.parentElement;

  // Создаём обёртку для кнопок навигации
  const navWrapper = document.createElement('div');
  navWrapper.className = 'ticket-nav-buttons-wrapper';
  navWrapper.style.cssText = `
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    margin-top: 12px;
  `;

  // Контейнер для кнопок
  const navContainer = document.createElement('div');
  navContainer.className = 'ticket-nav-buttons';
  navContainer.style.cssText = 'display: flex; gap: 8px; align-items: center;';

    // Кнопка "Предыдущий"
    const prevButton = this.createNavigationButton('prev', 'Предыдущий', 'chevron-left-outline-sm');
    prevButton.addEventListener('click', () => this.navigateToPrevious());

    // Кнопка "Следующий"
    const nextButton = this.createNavigationButton('next', 'Следующий', 'chevron-right-outline-sm');
    nextButton.addEventListener('click', () => this.navigateToNext());

    navContainer.appendChild(prevButton);
    navContainer.appendChild(nextButton);
    navWrapper.appendChild(navContainer);

    // Вставляем после контейнера с кнопкой "Назад"
    if (parentContainer) {
      parentContainer.insertBefore(navWrapper, backButtonContainer.nextSibling);
      console.log('✅ Ticket Navigation: Кнопки добавлены в DOM');
    } else {
      console.error('❌ Ticket Navigation: Не удалось найти родительский контейнер');
      return;
    }

    // Обновляем состояние
    await this.updateButtonsState();
  }


  // Создание кнопки навигации
  createNavigationButton(direction, text, iconId) {
    const button = document.createElement('button');
    button.className = `Button2 Button2_size_m Button2_view_clear ticket-nav-${direction}`;
    button.type = 'button';
    button.setAttribute('data-nav-direction', direction);

    const iconSide = direction === 'prev' ? 'left' : 'right';
    
    button.innerHTML = `
      <span class="Button2-Icon Button2-Icon_side_${iconSide}">
        <svg role="none" width="16" height="16">
          <use xlink:href="#${iconId}"></use>
        </svg>
      </span>
      <span class="Button2-Text">${text}</span>
    `;

    return button;
  }


  // Получить индекс текущего тикета
  async getCurrentTicketIndex() {
    if (!this.currentTicketId) {
      console.log('⚠️ Ticket Navigation: currentTicketId не установлен');
      return -1;
    }

    const list = await this.getNavigationList();
    
    if (list.length === 0) {
      console.log('⚠️ Ticket Navigation: Список тикетов пуст');
      return -1;
    }

    const index = list.indexOf(this.currentTicketId);
    
    console.log('📍 Ticket Navigation: Позиция тикета', {
      currentTicketId: this.currentTicketId,
      index,
      total: list.length,
      list
    });

    return index;
  }


  // Обновление состояния кнопок
  async updateButtonsState() {
    const prevButton = document.querySelector('[data-nav-direction="prev"]');
    const nextButton = document.querySelector('[data-nav-direction="next"]');

    if (!prevButton || !nextButton) {
      console.log('ℹ️ Ticket Navigation: Кнопки не найдены для обновления');
      return;
    }

    const currentIndex = await this.getCurrentTicketIndex();
    const list = await this.getNavigationList();
    
    console.log('🔄 Ticket Navigation: Обновление состояния кнопок', {
      currentIndex,
      totalTickets: list.length,
      currentTicketId: this.currentTicketId
    });
    
    // Кнопка "Предыдущий"
    if (currentIndex <= 0) {
      prevButton.disabled = true;
      prevButton.style.opacity = '0.5';
      prevButton.style.cursor = 'not-allowed';
      console.log('⬅️ Кнопка "Предыдущий": ОТКЛЮЧЕНА');
    } else {
      prevButton.disabled = false;
      prevButton.style.opacity = '1';
      prevButton.style.cursor = 'pointer';
      console.log('⬅️ Кнопка "Предыдущий": АКТИВНА');
    }

    // Кнопка "Следующий"
    if (currentIndex === -1 || currentIndex >= list.length - 1) {
      nextButton.disabled = true;
      nextButton.style.opacity = '0.5';
      nextButton.style.cursor = 'not-allowed';
      console.log('➡️ Кнопка "Следующий": ОТКЛЮЧЕНА');
    } else {
      nextButton.disabled = false;
      nextButton.style.opacity = '1';
      nextButton.style.cursor = 'pointer';
      console.log('➡️ Кнопка "Следующий": АКТИВНА');
    }
  }


  // Навигация к предыдущему тикету
  async navigateToPrevious() {
    const currentIndex = await this.getCurrentTicketIndex();
    
    if (currentIndex <= 0) {
      console.log('⚠️ Ticket Navigation: Невозможен переход назад');
      return;
    }

    const list = await this.getNavigationList();
    const previousTicketId = list[currentIndex - 1];
    
    console.log('⬅️ Ticket Navigation: Переход к предыдущему:', previousTicketId);
    await this.navigateToTicket(previousTicketId);
  }


  // Навигация к следующему тикету
  async navigateToNext() {
    const currentIndex = await this.getCurrentTicketIndex();
    const list = await this.getNavigationList();
    
    console.log('➡️ Ticket Navigation: Попытка перехода к следующему', {
      currentIndex,
      listLength: list.length,
      currentTicketId: this.currentTicketId,
      fullList: list
    });
    
    if (currentIndex === -1) {
      console.log('⚠️ Ticket Navigation: Текущий тикет не найден в списке!');
      console.log('Текущий ID:', this.currentTicketId);
      console.log('Список:', list);
      return;
    }
    
    if (currentIndex >= list.length - 1) {
      console.log('⚠️ Ticket Navigation: Это последний тикет в списке');
      return;
    }

    const nextTicketId = list[currentIndex + 1];
    
    console.log('➡️ Ticket Navigation: Переход к следующему тикету', {
      from: this.currentTicketId,
      fromIndex: currentIndex,
      to: nextTicketId,
      toIndex: currentIndex + 1
    });
    
    await this.navigateToTicket(nextTicketId);
  }


  // Навигация к конкретному тикету
  async navigateToTicket(ticketId) {
    console.log('🚀 Ticket Navigation: Переход к тикету:', ticketId);

    // Помечаем, что это навигация между тикетами
    window.dispatchEvent(new CustomEvent('ticketNavigationStarted'));

    const uid = this.uid || this.getUidFromUrl();
    const newUrl = `/support-center/${ticketId}${uid ? `?uid=${uid}` : ''}`;
    
    this.currentTicketId = ticketId;

    console.log('🔗 Ticket Navigation: Переход на URL:', newUrl);
    window.location.href = newUrl;
  }


  // Отслеживание изменений URL
  setupUrlWatcher() {
    console.log('👀 Ticket Navigation: Установка отслеживания URL');
    
    let lastUrl = window.location.href;

    const checkUrlChange = async () => {
      const currentUrl = window.location.href;
      
      if (currentUrl !== lastUrl) {
        console.log('🔄 Ticket Navigation: URL изменился', {
          from: lastUrl,
          to: currentUrl
        });
        
        // Проверяем переход из тикета в список
        const wasInTicket = lastUrl.includes('/support-center/') && 
                           lastUrl.split('/support-center/')[1].split(/[?/]/)[0];
        const nowInList = this.isTicketListPage();
        
        if (wasInTicket && nowInList) {
          console.log('🔄 Ticket Navigation: Переход из тикета в список, очищаем навигацию');
          await this.clearNavigationList();
        }
        
        lastUrl = currentUrl;
        await this.handleUrlChange();
      }
    };

    const urlObserver = new MutationObserver(checkUrlChange);
    urlObserver.observe(document, {
      subtree: true,
      childList: true
    });

    const originalPushState = history.pushState;
    const originalReplaceState = history.replaceState;

    history.pushState = function(...args) {
      originalPushState.apply(this, args);
      checkUrlChange();
    };

    history.replaceState = function(...args) {
      originalReplaceState.apply(this, args);
      checkUrlChange();
    };

    window.addEventListener('popstate', checkUrlChange);
  }


  // Обработка изменения URL
  async handleUrlChange() {
    console.log('🔄 Ticket Navigation: Обработка изменения URL');
    
    if (this.isTicketViewPage()) {
      const ticketId = this.getTicketIdFromUrl();
      
      // Всегда удаляем и добавляем кнопки заново
      console.log('📍 Ticket Navigation: Страница тикета, текущий ID:', ticketId);
      this.currentTicketId = ticketId;
      this.injectionAttempts = 0;
      
      // Удаляем старые кнопки
      this.removeNavigationButtons();
      
      // Добавляем кнопки если функция включена
      if (this.isEnabled) {
        await this.waitAndInjectButtons();
      }
    } else {
      console.log('📄 Ticket Navigation: Не на странице тикета');
      this.removeNavigationButtons();
    }
  }


  // Удаление кнопок навигации
  removeNavigationButtons() {
    const navWrapper = document.querySelector('.ticket-nav-buttons-wrapper');
    if (navWrapper) {
      navWrapper.remove();
      console.log('🗑️ Ticket Navigation: Кнопки удалены');
    }
  }
}

// Создание и инициализация
console.log('🎯 Ticket Navigation: Создание экземпляра');
window.ticketNavigation = new TicketNavigation();

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    window.ticketNavigation.init();
  });
} else {
  window.ticketNavigation.init();
}