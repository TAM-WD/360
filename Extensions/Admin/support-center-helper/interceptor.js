// Runs in MAIN at document_start. Only network observation belongs in the page world.
(() => {
  const EVENT = 'supportHelperNetwork';
  let ready = false;
  const pending = [];
  const LIST = 'support-b2b/get-tickets';
  const DETAIL = 'support-b2b/get-ticket';

  function emit(payload) {
    // JSON also prevents passing live page objects into the isolated content script.
    const detail = JSON.stringify(payload);
    if (ready) window.dispatchEvent(new CustomEvent(EVENT, {detail}));
    else pending.push(detail);
  }
  window.addEventListener('supportHelperReady', () => {
    ready = true;
    for (const detail of pending.splice(0)) window.dispatchEvent(new CustomEvent(EVENT, {detail}));
  });
  window.dispatchEvent(new CustomEvent('supportHelperInterceptorReady'));

  function requests(url, body) {
    const parsedUrl = new URL(url, location.href);
    let parsed = {};
    if (typeof body === 'string' && body) {
      try { parsed = JSON.parse(body); } catch { return []; }
    }
    if (Array.isArray(parsed.models)) {
      return parsed.models.filter(model => [LIST, DETAIL].includes(model.name));
    }
    const name = parsedUrl.pathname.includes(LIST) ? LIST : parsedUrl.pathname.includes(DETAIL) ? DETAIL : null;
    return name ? [{name, params: parsed.params || {...Object.fromEntries(parsedUrl.searchParams), ...parsed}}] : [];
  }

  function hasError(value) {
    if (Array.isArray(value)) return value.length > 0;
    if (value && typeof value === 'object') return Object.keys(value).length > 0;
    return Boolean(value);
  }

  function processResponse(models, data, requestedAt, uid, pageUrl) {
    for (const request of models) {
      // Match by model name, not models[0]; a batch can contain unrelated models.
      let model = data;
      if (Array.isArray(data?.models)) {
        const candidates = data.models.filter(item => item?.name === request.name);
        model = request.id != null ? candidates.find(item => item.id != null && String(item.id) === String(request.id)) : null;
        // Some model envelopes omit ID in the response. Name alone is safe only
        // when both sides contain one unambiguous instance of that model.
        if (!model && candidates.length === 1 && models.filter(item => item.name === request.name).length === 1 &&
            (request.id == null || candidates[0].id == null)) model = candidates[0];
      }
      if (!model || hasError(model.error) || hasError(model.errors)) continue;
      const status = String(model.status ?? '').toLowerCase();
      if (['error', 'failed', 'failure', 'fail'].includes(status) || Number(status) >= 400) continue;
      const payload = model.data;
      const params = request.params || {};
      const context = {uid, orgId: params.org_id || ''};
      if (request.name === LIST && Array.isArray(payload?.items)) {
        emit({action: 'captureTickets', tickets: payload.items, filters: params, context, requestedAt,
          pageUrl});
      } else if (request.name === DETAIL && payload && typeof payload === 'object') {
        const ticket = payload?.ticket || payload?.item || payload;
        emit({action: 'captureTicket', context, requestedAt, pageUrl,
          currentTicketId: params.ticket_id ?? ticket?.ticket_id,
          ticket: ticket && typeof ticket === 'object' ? {...ticket, ticket_id: ticket.ticket_id || params.ticket_id} : null});
      }
    }
  }

  function requestUid(headers) {
    const fromUrl = new URLSearchParams(location.search).get('uid');
    if (fromUrl) return fromUrl;
    try { return new Headers(headers).get('x-sarah-uid') || ''; } catch { return ''; }
  }

  const originalFetch = window.fetch;
  window.fetch = function(input, options = {}) {
    options = options || {};
    const url = typeof input === 'string' || input instanceof URL ? String(input) : input?.url;
    const requestedAt = performance.timeOrigin + performance.now();
    const pageUrl = location.href;
    const uid = requestUid(options.headers || (input instanceof Request ? input.headers : undefined));
    const relevant = url && (url.includes('/api/models') || url.includes('support-b2b/get-ticket'));
    let models;
    if (relevant) {
      // Read a clone: consuming a Request body would break the site's original fetch.
      const body = options.body !== undefined ? Promise.resolve(options.body) :
        input instanceof Request ? input.clone().text() : Promise.resolve('');
      models = body.then(value => requests(url, value)).catch(() => []);
    }
    const result = originalFetch.apply(this, arguments);
    if (relevant) {
      result.then(response => {
        if (!response.ok) return;
        // Start cloning immediately, before the caller can consume the response body.
        const data = response.clone().json();
        Promise.all([models, data]).then(([batch, json]) => {
          processResponse(batch, json, requestedAt, uid, pageUrl);
        }).catch(() => {});
      }).catch(() => {});
    }
    return result;
  };

  const originalOpen = XMLHttpRequest.prototype.open;
  const originalSend = XMLHttpRequest.prototype.send;
  const originalSetHeader = XMLHttpRequest.prototype.setRequestHeader;
  const requestUrls = new WeakMap();
  const requestUids = new WeakMap();
  XMLHttpRequest.prototype.open = function(method, url, ...rest) {
    requestUrls.set(this, String(url));
    requestUids.delete(this);
    return originalOpen.call(this, method, url, ...rest);
  };
  XMLHttpRequest.prototype.setRequestHeader = function(name, value) {
    const result = originalSetHeader.apply(this, arguments);
    if (String(name).toLowerCase() === 'x-sarah-uid') requestUids.set(this, String(value));
    return result;
  };
  XMLHttpRequest.prototype.send = function(body) {
    const url = requestUrls.get(this) || '';
    if (url.includes('/api/models') || url.includes('support-b2b/get-ticket')) {
      const requestedAt = performance.timeOrigin + performance.now();
      const pageUrl = location.href;
      const uid = requestUid({'x-sarah-uid': requestUids.get(this) || ''});
      const models = requests(url, body);
      const onLoad = () => {
        if (this.status < 200 || this.status >= 300) return;
        try {
          const data = this.responseType === 'json' ? this.response : JSON.parse(this.responseText);
          processResponse(models, data, requestedAt, uid, pageUrl);
        } catch { /* Non-JSON/error responses do not replace a successfully collected list. */ }
      };
      this.addEventListener('load', onLoad, {once: true});
      this.addEventListener('loadend', () => this.removeEventListener('load', onLoad), {once: true});
    }
    return originalSend.apply(this, arguments);
  };
})();
