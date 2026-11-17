const ROOT = '/telemost_admin_front/v1/telemost-api/admin';
const PATH = /^\/conferences(?:\/[^/?#]+\/sessions\/[^/?#]+\/(?:info|participants(?:\/[^/?#]+(?:\/stats\/(?:publisher|subscriber)\/(?:audio|video|sharing-video))?)?))?$/;

export function createTransport(context, orgId) {
  if (!context || String(context.orgId) !== String(orgId)) throw new Error('Выберите организацию в кабинете и обновите аналитику.');
  return async function request(path, params = {}, signal) {
    if (!PATH.test(path)) throw new Error('Неподдерживаемый путь API.');
    signal?.throwIfAborted();
    const query = new URLSearchParams({...params, org_id: String(orgId)}).toString();
    let injected;
    try {
      injected = await chrome.scripting.executeScript({
        target: {tabId: context.tabId, frameIds: [context.frameId]},
        // ISOLATED world keeps fetch independent from the site's JS wrappers.
        world: 'ISOLATED',
        func: async (path, query, expectedOrg) => {
          if (!['https://admin.telemost.yandex.ru', 'https://admin.yandex.ru'].includes(location.origin)) return {error: 'context'};
          const frameOrg = new URL(location.href).searchParams.get('orgId');
          if (frameOrg && frameOrg !== expectedOrg) return {error: 'organization'};
          if (location.origin === 'https://admin.telemost.yandex.ru') {
            try {
              const config = JSON.parse(document.querySelector('#preloaded-state')?.textContent || '{}').config;
              if (config?.orgId != null && String(config.orgId) !== expectedOrg) return {error: 'organization'};
            } catch { /* URL and observed request remain the context checks. */ }
          }
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 25000);
          try {
            const response = await fetch('https://cloud-api.yandex.ru' + path + '?' + query, {
              method: 'GET', credentials: 'include', headers: {Accept: 'application/json'},
              signal: controller.signal, redirect: 'error'
            });
            if (!response.ok) return {status: response.status, retryAfter: response.headers.get('Retry-After')};
            try { return {status: response.status, data: await response.json()}; }
            catch { return {error: 'json'}; }
          } catch { return {error: controller.signal.aborted ? 'timeout' : 'network'}; }
          finally { clearTimeout(timeout); }
        },
        args: [ROOT + path, query, String(orgId)]
      });
    } catch {
      const error = new Error('Вкладка аналитики недоступна. Откройте её и обновите страницу, затем запустите новую выгрузку.');
      error.fatal = true; throw error;
    }
    signal?.throwIfAborted();
    const result = injected?.[0]?.result;
    if (!result || result.error) {
      const error = new Error(({context: 'Откройте вкладку аналитики.', organization: 'Организация во вкладке изменилась. Запустите новую выгрузку.', json: 'API вернул ответ не в формате JSON.', timeout: 'API не ответил за 25 секунд.', network: 'Не удалось получить ответ API. Проверьте вход в кабинет.'})[result?.error] || 'Нет ответа из вкладки аналитики.');
      error.fatal = ['context', 'organization'].includes(result?.error);
      throw error;
    }
    if (!result.data) {
      const status = result.status;
      const error = new Error(status === 401 || status === 403 ? 'Сессия истекла или нет прав на аналитику. Войдите в кабинет и запустите новую выгрузку.' : `API ответил HTTP ${status}.`);
      error.status = status; error.fatal = status === 401 || status === 403;
      const retry = Number(result.retryAfter);
      if (Number.isFinite(retry) && retry > 0) error.retryMs = Math.min(retry * 1000, 60000);
      throw error;
    }
    return result.data;
  };
}
