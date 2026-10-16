// Runs in the isolated content-script world, alongside content.js.
class TicketNavigation {
  constructor() {
    this.enabled = false;
    this.lastUrl = '';
    this.wrapper = null;
    this.renderVersion = 0;
    this.pending = null;
    this.moving = false;
  }

  start() {
    // Watch indefinitely: the app may render late or replace its header after a transition.
    this.observer = new MutationObserver(() => {
      if (location.href !== this.lastUrl || (this.enabled && this.ticketId() && !this.wrapper?.isConnected)) {
        this.schedule();
      }
    });
    this.observer.observe(document, {childList: true, subtree: true});
    window.addEventListener('popstate', () => this.schedule());
    window.addEventListener('pageshow', () => this.schedule());
    this.urlTimer = setInterval(() => {
      if (location.href !== this.lastUrl) this.schedule();
    }, 500);
    this.schedule();
  }

  setEnabled(enabled) {
    this.enabled = Boolean(enabled);
    this.schedule();
  }

  ticketId() {
    const match = location.pathname.match(/^\/support-center\/([^/]+)\/?$/);
    if (!match) return null;
    try { return decodeURIComponent(match[1]); } catch { return null; }
  }

  setCurrentTicket(id, pageUrl, requestedAt) {
    // A late response for the previous SPA route must never move the current anchor.
    if (!['string', 'number'].includes(typeof id) || pageUrl !== location.href) return;
    if (this.currentTicket?.pageUrl === pageUrl && this.currentTicket.requestedAt > requestedAt) return;
    this.currentTicket = {id: String(id), pageUrl, requestedAt};
  }

  currentId() {
    return this.currentTicket?.pageUrl === location.href ? this.currentTicket.id : this.ticketId();
  }

  context() {
    const params = new URLSearchParams(location.search);
    return {uid: params.get('uid') || '', orgId: params.get('org_id') || ''};
  }

  schedule() {
    // Invalidate an outstanding read immediately; it must not enable stale buttons.
    this.renderVersion++;
    clearTimeout(this.pending);
    this.pending = setTimeout(() => this.render(), 50);
  }

  async getList() {
    const response = await chrome.runtime.sendMessage({action: 'getNavigationList', context: this.context(),
      currentTicketId: this.currentId(), routeTicketId: this.ticketId()});
    if (!response?.success) throw new Error(response?.error || 'Не удалось прочитать список тикетов');
    this.routingContext = response.context || {};
    this.metadata = response.metadata || {};
    this.resolvedTicketId = response.currentTicketId || this.currentId();
    return response.tickets;
  }

  diagnostics() {
    return {enabled: this.enabled, ticketPage: Boolean(this.ticketId()),
      detailIdReceived: this.currentTicket?.pageUrl === location.href,
      currentFound: this.metadata?.currentFound ?? null, routeFound: this.metadata?.routeFound ?? null,
      previousEnabled: this.wrapper?.querySelector('[data-nav-direction="prev"]')?.disabled === false,
      nextEnabled: this.wrapper?.querySelector('[data-nav-direction="next"]')?.disabled === false};
  }

  async render() {
    const version = this.renderVersion;
    this.lastUrl = location.href;
    const ticketId = this.ticketId();
    if (!this.enabled || !ticketId) {
      this.wrapper?.remove();
      this.wrapper = null;
      return;
    }
    const back = document.querySelector('[data-testid="back-button"]') ||
      [...document.querySelectorAll('button, a')].find(button => button.textContent.trim().startsWith('Назад'));
    if (!back?.parentElement) return;
    if (!this.wrapper?.isConnected) {
      this.wrapper = document.createElement('div');
      this.wrapper.className = 'ticket-nav-buttons-wrapper';
      const buttons = document.createElement('div');
      buttons.className = 'ticket-nav-buttons';
      for (const [direction, label] of [['prev', 'Предыдущий'], ['next', 'Следующий']]) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = `Button2 Button2_size_m Button2_view_clear ticket-nav-${direction}`;
        button.dataset.navDirection = direction;
        const arrow = document.createElement('span');
        arrow.className = 'ticket-nav-arrow';
        arrow.setAttribute('aria-hidden', 'true');
        arrow.textContent = direction === 'prev' ? '‹' : '›';
        const text = document.createElement('span');
        text.textContent = label;
        button.append(...(direction === 'prev' ? [arrow, text] : [text, arrow]));
        button.disabled = true;
        button.addEventListener('click', () => this.navigate(direction === 'next' ? 1 : -1));
        buttons.append(button);
      }
      this.wrapper.append(buttons);
      back.parentElement.insertAdjacentElement('afterend', this.wrapper);
    }
    const wrapper = this.wrapper;
    try {
      const list = await this.getList();
      if (version !== this.renderVersion || location.href !== this.lastUrl || !wrapper.isConnected) return;
      const index = list.indexOf(this.resolvedTicketId);
      const reason = this.metadata.contextMismatch ? 'Сохранённый список относится к другой организации или учётной записи' :
        !this.metadata.total ? 'Нет сохранённого списка. Откройте список тикетов в этой вкладке' :
        index < 0 ? `Текущего тикета нет в сохранённом списке (${this.metadata.total})` :
        list.length === 1 ? 'Нет других тикетов с учётом фильтров расширения' : '';
      for (const button of wrapper.querySelectorAll('button')) {
        const delta = button.dataset.navDirection === 'next' ? 1 : -1;
        button.disabled = index < 0 || !list[index + delta];
        button.title = button.disabled ? reason || (delta < 0 ? 'Это первый тикет списка' : 'Это последний тикет списка') : '';
      }
    } catch (error) {
      if (version !== this.renderVersion || location.href !== this.lastUrl || !wrapper.isConnected) return;
      for (const button of wrapper.querySelectorAll('button')) {
        button.disabled = true;
        button.title = error.message;
      }
    }
  }

  async navigate(delta) {
    if (this.moving) return;
    this.moving = true;
    const currentUrl = location.href;
    try {
      const list = await this.getList();
      if (location.href !== currentUrl || !this.enabled) return;
      const index = list.indexOf(this.resolvedTicketId);
      const next = index >= 0 ? list[index + delta] : null;
      if (!next) { this.schedule(); return; }
      const url = new URL(currentUrl);
      url.pathname = `/support-center/${encodeURIComponent(next)}`;
      if (!url.searchParams.has('uid') && this.routingContext.uid) {
        url.searchParams.set('uid', this.routingContext.uid);
      }
      // Preserve uid/org and other routing parameters, so the destination keeps its context.
      window.location.assign(url.href);
    } catch (error) {
      console.error('Support Center Helper: переход не выполнен', error.message);
      this.schedule();
    } finally {
      this.moving = false;
    }
  }
}
