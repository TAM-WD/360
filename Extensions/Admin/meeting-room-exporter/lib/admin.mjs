// Runs inside the explicitly selected admin tab. Never returns session credentials.
export async function readAdminCatalog(options = {}) {
  const fail = (message) => ({ok: false, message});
  if (location.origin !== 'https://admin.yandex.ru') {
    return fail('Откройте админку https://admin.yandex.ru, войдите и нажмите значок расширения на этой вкладке.');
  }
  let config, store;
  try {
    config = JSON.parse(document.getElementById('sarah-config')?.textContent || 'null');
    store = JSON.parse(document.getElementById('initial-store')?.textContent || 'null');
  } catch {
    return fail('Не удалось прочитать данные страницы. Обновите вкладку админки и запустите выгрузку снова.');
  }
  if (!config || !store) return fail('На странице нет данных админки. Откройте раздел «Переговорные» и повторите.');
  const asOrgId = (value) => {
    if (typeof value !== 'string' && typeof value !== 'number') return null;
    if (!/^\d+$/.test(String(value))) return null;
    const id = Number(value);
    return Number.isSafeInteger(id) && id > 0 ? id : null;
  };
  const organizations = Object.entries(store.organizations?.byId || {}).flatMap(([key, value]) => {
    const id = asOrgId(value?.id ?? key);
    return id ? [{id, name: String(value?.name || `Организация ${id}`)}] : [];
  });
  let cookieOrg;
  try {
    const cookie = document.cookie.split(';').find((item) => item.trim().startsWith('org_id='));
    cookieOrg = cookie ? decodeURIComponent(cookie.trim().slice('org_id='.length)) : null;
  } catch { cookieOrg = null; }
  const activeOrgId = asOrgId(new URL(location.href).searchParams.get('org_id')) || asOrgId(cookieOrg);
  if (options.mode === 'probe') return {ok: true, organizations, activeOrgId};
  const orgId = asOrgId(options.orgId) || activeOrgId;
  if (!orgId) return fail('Не удалось определить организацию. Выберите её в списке или укажите ID организации.');
  const headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
    'X-Retpath-Y': location.href,
  };
  const prefix = config.headerPrefix;
  if (typeof prefix !== 'string' || !/^[A-Za-z][A-Za-z0-9-]{0,50}$/.test(prefix) || !config.ckey) {
    return fail('Не найден ключ текущей сессии. Обновите вкладку админки и повторите.');
  }
  for (const [key, value] of Object.entries({cid: config.connectionId, ckey: config.ckey, locale: config.i18n?.lang, uid: store.user?.uid})) {
    if (value !== undefined && value !== null) headers[prefix + key] = String(value);
  }
  const request = async (name) => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 45000);
    try {
      const url = new URL('/api/models', location.origin);
      url.searchParams.set('_models', name);
      const response = await fetch(url.href, {
        method: 'POST', credentials: 'include', redirect: 'error', headers,
        body: JSON.stringify({models: [{name, params: {org_id: orgId}}]}),
        signal: controller.signal,
      });
      if (!response.ok) {
        if (response.status === 401 || response.status === 403) throw new Error('AUTH');
        throw new Error('HTTP');
      }
      let payload;
      try { payload = await response.json(); } catch { throw new Error('FORMAT'); }
      const model = payload?.models?.find((item) => item.name === name);
      if (!model || model.error || model.status !== 'ok' || !Array.isArray(model.data)) throw new Error('MODEL');
      // Only resource data crosses the extension boundary; the envelope contains session keys.
      return model.data;
    } finally { clearTimeout(timeout); }
  };
  try {
    const rooms = await request('directory/get-rooms');
    const warnings = [];
    let offices = [];
    try { offices = await request('directory/get-offices'); }
    catch { warnings.push('Справочник офисов недоступен: используются офисы из карточек комнат. Дополнительные адреса могут отсутствовать.'); }
    return {ok: true, rooms, offices, orgId, source: 'live', warnings};
  } catch (error) {
    if (error?.name === 'AbortError') return fail('Сервер не ответил за 45 секунд. Повторите выгрузку.');
    if (error?.message === 'AUTH') return fail('Нет доступа или сессия истекла. Войдите в админку и обновите исходную вкладку.');
    if (error?.message === 'MODEL') return fail('API отклонил запрос или изменил формат. Обновите исходную вкладку и проверьте доступ к выбранной организации.');
    if (error?.message === 'FORMAT') return fail('Сервер вернул неожиданный ответ. Обновите исходную вкладку и войдите в админку.');
    return fail('Не удалось загрузить каталог. Проверьте соединение и повторите.');
  }
}
