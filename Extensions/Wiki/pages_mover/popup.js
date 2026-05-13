document.addEventListener('DOMContentLoaded', async () => {

  const tabs      = await chrome.tabs.query({ active: true, currentWindow: true });
  const activeTab = tabs[0];

  if (!activeTab?.url?.startsWith('https://wiki.yandex.ru/')) {
    document.getElementById('wrong-page').style.display   = 'block';
    document.getElementById('main-content').style.display = 'none';
    return;
  }

  let foundPages      = [];
  let isRunning       = false;
  let capturedHeaders = null;

  const btnScan          = document.getElementById('btn-scan');
  const btnMove          = document.getElementById('btn-move');
  const btnSelectAll     = document.getElementById('btn-select-all');
  const btnUseCurrent    = document.getElementById('btn-use-current');
  const btnUseCurrentTgt = document.getElementById('btn-use-current-target');
  const btnSwap          = document.getElementById('btn-swap');
  const btnReload        = document.getElementById('btn-reload');
  const reloadBar        = document.getElementById('reload-bar');
  const pagesList        = document.getElementById('pages-list');
  const logSection       = document.getElementById('log-section');
  const logArea          = document.getElementById('log-area');
  const progressFill     = document.getElementById('progress-fill');
  const sourceHint       = document.getElementById('source-hint');
  const targetHint       = document.getElementById('target-hint');
  const sourceInput      = document.getElementById('source-path');
  const targetInput      = document.getElementById('target-path');
  const dialogOverlay    = document.getElementById('dialog-overlay');
  const btnDialogYes     = document.getElementById('btn-dialog-yes');
  const btnDialogNo      = document.getElementById('btn-dialog-no');

  const getSource = () => sourceInput.value.trim().replace(/^\/|\/$/g, '');
  const getTarget = () => targetInput.value.trim().replace(/^\/|\/$/g, '');

  function slugFromUrl(url) {
    try {
      const parts = new URL(url).pathname.replace(/^\/|\/$/g, '').split('/');
      return parts[0] || '';
    } catch (_) { return ''; }
  }

  function updateHint(inputId, hintEl) {
    const val = document.getElementById(inputId).value.trim().replace(/^\/|\/$/g, '');
    hintEl.textContent = val
      ? `wiki.yandex.ru/${val}/`
      : 'Введите название раздела или нажмите "Текущая"';
    hintEl.classList.remove('hint-error');
  }

  function validatePaths() {
    const src = getSource();
    const tgt = getTarget();
    const same = src && tgt && src === tgt;

    sourceInput.classList.toggle('input-error', same);
    targetInput.classList.toggle('input-error', same);

    if (same) {
      sourceHint.textContent = 'Папка-источник и назначение совпадают!';
      sourceHint.classList.add('hint-error');
      targetHint.textContent = 'Папка-источник и назначение совпадают!';
      targetHint.classList.add('hint-error');
    } else {
      updateHint('source-path', sourceHint);
      updateHint('target-path', targetHint);
    }

    return !same;
  }

  sourceInput.addEventListener('input', validatePaths);
  targetInput.addEventListener('input', validatePaths);

  function askInheritance(pageName) {
    return new Promise(resolve => {
      document.getElementById('dialog-text').innerHTML =
        `Страница <b>${pageName}</b> имеет другие права доступа.<br>
         Скопировать права из папки назначения?`;
      dialogOverlay.classList.add('visible');

      const yes = () => { cleanup(); resolve(true);  };
      const no  = () => { cleanup(); resolve(false); };

      function cleanup() {
        dialogOverlay.classList.remove('visible');
        btnDialogYes.removeEventListener('click', yes);
        btnDialogNo.removeEventListener('click',  no);
      }

      btnDialogYes.addEventListener('click', yes);
      btnDialogNo.addEventListener('click',  no);
    });
  }

  btnUseCurrent.addEventListener('click', async () => {
    const tab  = await getTab();
    const slug = slugFromUrl(tab.url);
    if (slug) {
      sourceInput.value = slug;
      validatePaths();
      saveSettings();
    } else {
      alert('Не удалось определить раздел.');
    }
  });

  btnUseCurrentTgt.addEventListener('click', async () => {
    const tab  = await getTab();
    const slug = slugFromUrl(tab.url);
    if (slug) {
      targetInput.value = slug;
      validatePaths();
      saveSettings();
    } else {
      alert('Не удалось определить раздел.');
    }
  });

  btnSwap.addEventListener('click', () => {
    const tmp         = sourceInput.value;
    sourceInput.value = targetInput.value;
    targetInput.value = tmp;
    validatePaths();
    saveSettings();
    foundPages = [];
    pagesList.innerHTML = '<div class="empty-hint">Нажмите "Сканировать"</div>';
    btnMove.disabled = true;
    reloadBar.style.display = 'none';
  });

  btnReload.addEventListener('click', async () => {
    const tab = await getTab();
    if (tab) chrome.tabs.reload(tab.id);
  });

  function log(msg, type = 'info') {
    logSection.style.display = 'block';
    const line = document.createElement('div');
    line.className = `log-line ${type}`;
    line.textContent = msg;
    logArea.appendChild(line);
    logArea.scrollTop = logArea.scrollHeight;
  }

  function setProgress(pct) {
    progressFill.style.width = `${Math.min(100, Math.max(0, pct))}%`;
  }

  function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  async function getTab() {
    const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
    return tabs[0];
  }

  function setStatus(idx, type, sym, title) {
    const el = document.getElementById(`status-${idx}`);
    if (!el) return;
    el.className = `page-status status-${type}`;
    el.textContent = sym;
    el.title = title;
  }

  function injectFetchInterceptor() {
    if (window.__wikiMoverInterceptor) return;
    window.__wikiMoverInterceptor = true;
    window.__wikiMoverHeaders = null;
    const _fetch = window.fetch.bind(window);
    window.fetch = async function(input, init = {}) {
      const url = typeof input === 'string' ? input : (input?.url || '');
      if (url.includes('.gateway/root/wiki') || url.includes('/wiki/')) {
        const h   = init.headers || {};
        const get = (name) => h instanceof Headers
          ? h.get(name) || ''
          : h[name] || h[name.toLowerCase()] || '';
        const orgId       = get('x-org-id');
        const collabOrgId = get('x-collab-org-id');
        const csrfToken   = get('x-csrf-token');
        if (orgId || collabOrgId) {
          window.__wikiMoverHeaders = { orgId, collabOrgId, csrfToken };
        }
      }
      return _fetch(input, init);
    };
  }

  function extractHeadersFromPage() {
    if (window.__wikiMoverHeaders?.orgId) {
      const h       = window.__wikiMoverHeaders;
      const csrfRaw = document.cookie.split(';').map(c => c.trim())
        .find(c => c.startsWith('CSRF-TOKEN=')) || '';
      h.csrfToken   = decodeURIComponent(csrfRaw.replace('CSRF-TOKEN=', ''));
      return h;
    }
    let orgId = '', collabOrgId = '';
    for (const src of [window.__data, window.__APP_STATE__, window.__INITIAL_STATE__].filter(Boolean)) {
      try {
        const str = JSON.stringify(src);
        const m1  = str.match(/"(?:orgId|org_id)"\s*:\s*"?(\d{5,})"?/);
        const m2  = str.match(/"(?:collabOrgId|collab_org_id)"\s*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"/i);
        if (m1) orgId       = m1[1];
        if (m2) collabOrgId = m2[1];
        if (orgId) break;
      } catch (_) {}
    }
    if (!orgId) {
      document.querySelectorAll('script').forEach(s => {
        const t = s.textContent || '';
        if (!t.includes('orgId')) return;
        const m1 = t.match(/"(?:orgId|org_id)"\s*:\s*"?(\d{5,})"?/);
        const m2 = t.match(/"(?:collabOrgId|collab_org_id)"\s*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"/i);
        if (m1 && !orgId)       orgId       = m1[1];
        if (m2 && !collabOrgId) collabOrgId = m2[1];
      });
    }
    if (!orgId) return null;
    const csrfRaw   = document.cookie.split(';').map(c => c.trim())
      .find(c => c.startsWith('CSRF-TOKEN=')) || '';
    const csrfToken = decodeURIComponent(csrfRaw.replace('CSRF-TOKEN=', ''));
    return { orgId, collabOrgId, csrfToken };
  }

  async function ensureHeaders() {
    if (capturedHeaders?.orgId) return capturedHeaders;
    const tab = await getTab();
    if (!tab) return null;
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, func: injectFetchInterceptor });
    const results = await chrome.scripting.executeScript({ target: { tabId: tab.id }, func: extractHeadersFromPage });
    const headers = results[0]?.result;
    if (headers?.orgId) capturedHeaders = headers;
    return capturedHeaders;
  }

  function scanTreeInPage(sourceSlug) {
    return new Promise(resolve => {
      const pages   = [];
      const seen    = new Set();
      const srcPath = '/' + sourceSlug + '/';

      function collect() {
        document.querySelectorAll('.NavigationTree-Item').forEach(item => {
          const link = item.querySelector('.NavigationTree-ItemTitle');
          if (!link) return;
          let href = link.getAttribute('href') || '';
          if (!href.endsWith('/')) href += '/';
          if (seen.has(href)) return;
          if (href.startsWith(srcPath) && href !== srcPath) {
            const rest = href.slice(srcPath.length).replace(/\/$/, '');
            if (rest && !rest.includes('/')) {
              seen.add(href);
              const titleEl = link.querySelector('.NavigationTree-ItemTitleText');
              pages.push({
                title : titleEl ? titleEl.textContent.trim() : rest,
                href,
                slug  : rest,
                source: sourceSlug + '/' + rest
              });
            }
          }
        });
      }

      const sourceItem = [...document.querySelectorAll('.NavigationTree-Item')]
        .find(item => {
          const link = item.querySelector('.NavigationTree-ItemTitle');
          if (!link) return false;
          let href = link.getAttribute('href') || '';
          if (!href.endsWith('/')) href += '/';
          return href === srcPath;
        });

      if (sourceItem?.querySelector('.NavigationTree-ItemIconArrow_closed')) {
        sourceItem.querySelector('.NavigationTree-ItemIcon')?.click();
      }

      const scroller = document.querySelector('.NavigationTreeScrollable');
      if (!scroller) { collect(); resolve(pages); return; }

      const inner       = scroller.querySelector(':scope > div');
      const totalHeight = inner
        ? parseInt(inner.style.height || '0', 10)
        : scroller.scrollHeight;
      const step = scroller.clientHeight || 200;
      let   pos  = 0;
      scroller.scrollTop = 0;

      function tick() {
        collect();
        pos += step;
        if (pos <= totalHeight) {
          scroller.scrollTop = pos;
          setTimeout(tick, 200);
        } else {
          collect();
          scroller.scrollTop = 0;
          resolve(pages);
        }
      }
      setTimeout(tick, 400);
    });
  }

  async function callMoveAPI(operations, orgId, collabOrgId, csrfToken, copyInheritedAccess) {
    const controller = new AbortController();
    const timeout    = setTimeout(() => controller.abort(), 15000);
    try {
      const res = await fetch('/.gateway/root/wiki/movePages', {
        method     : 'POST',
        credentials: 'include',
        signal     : controller.signal,
        headers    : {
          'Content-Type'    : 'application/json',
          'Accept'          : 'application/json, text/plain, */*',
          'x-csrf-token'    : csrfToken,
          'x-org-id'        : String(orgId),
          'x-collab-org-id' : collabOrgId,
        },
        body: JSON.stringify({
          operations,
          copyInheritedAccess: copyInheritedAccess ?? false,
          checkInheritance   : true,
          dryRun             : false
        })
      });
      clearTimeout(timeout);
      const text = await res.text();
      let data;
      try   { data = JSON.parse(text); }
      catch { data = text; }
      return { ok: res.ok, status: res.status, data };
    } catch (err) {
      clearTimeout(timeout);
      return {
        ok    : false,
        status: 0,
        data  : { error: err.name === 'AbortError' ? 'Таймаут (15 сек)' : err.message }
      };
    }
  }

  async function pollMoveStatus(taskId, orgId, collabOrgId, csrfToken) {
    for (let i = 0; i < 30; i++) {
      await new Promise(r => setTimeout(r, 1000));
      try {
        const res = await fetch('/.gateway/root/wiki/getMoveOperationStatus', {
          method     : 'POST',
          credentials: 'include',
          headers    : {
            'Content-Type'    : 'application/json',
            'Accept'          : 'application/json, text/plain, */*',
            'x-csrf-token'    : csrfToken,
            'x-org-id'        : String(orgId),
            'x-collab-org-id' : collabOrgId,
          },
          body: JSON.stringify({ taskId })
        });
        const data   = await res.json().catch(() => ({}));
        const status = data?.status || '';
        if (status === 'success') return { ok: true,  pages: data?.result?.page_count };
        if (status === 'error' || status === 'failed') return { ok: false, error: status };
      } catch (_) {}
    }
    return { ok: false, error: 'Таймаут ожидания' };
  }

  async function scanPages() {
    if (isRunning) return;

    if (!validatePaths()) {
      alert('Папка-источник и папка назначения не могут совпадать!');
      return;
    }

    const source = getSource();
    if (!source) { alert('Укажите папку-источник!'); return; }

    btnScan.textContent = 'Сканирую...';
    btnScan.disabled    = true;
    foundPages          = [];
    reloadBar.style.display = 'none';

    try {
      const tab = await getTab();
      if (!tab) throw new Error('Нет активной вкладки');

      await chrome.scripting.executeScript({ target: { tabId: tab.id }, func: injectFetchInterceptor });

      const scanResults = await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        func  : scanTreeInPage,
        args  : [source]
      });

      const pages = scanResults[0]?.result || [];
      foundPages  = pages;
      renderList(pages);

      if (pages.length === 0) {
        log('Страниц не найдено. Убедитесь что папка раскрыта в дереве.', 'warn');
      } else {
        log(`Найдено страниц: ${pages.length}`, 'success');
      }

      capturedHeaders = null;
      await ensureHeaders();

      if (!capturedHeaders?.orgId) {
        log('Не удалось получить данные автоматически. Кликните по любой странице в дереве и нажмите Сканировать снова.', 'warn');
      }

    } catch (err) {
      log(`Ошибка: ${err.message}`, 'error');
    } finally {
      btnScan.textContent = 'Сканировать';
      btnScan.disabled    = false;
    }
  }

  function renderList(pages) {
    pagesList.innerHTML = '';
    if (pages.length === 0) {
      pagesList.innerHTML = '<div class="empty-hint">Страниц не найдено</div>';
      btnMove.disabled = true;
      return;
    }
    const target = getTarget();
    pages.forEach((p, i) => {
      const div = document.createElement('div');
      div.className = 'page-item';
      div.innerHTML = `
        <input type="checkbox" id="chk-${i}" checked>
        <div class="page-info">
          <div class="page-title">${escapeHtml(p.title)}</div>
          <div class="page-url">/${escapeHtml(p.source)}/ → /${escapeHtml(target + '/' + p.slug)}/</div>
        </div>
        <div class="page-status status-pending" id="status-${i}" title="Ожидание">·</div>
      `;
      pagesList.appendChild(div);
    });
    btnMove.disabled = false;
  }

  function getSelected() {
    return foundPages.filter((_, i) => document.getElementById(`chk-${i}`)?.checked);
  }

  function toggleAll() {
    const boxes = [...pagesList.querySelectorAll('input[type=checkbox]')];
    const all   = boxes.every(b => b.checked);
    boxes.forEach(b => b.checked = !all);
  }

  async function movePages() {
    if (isRunning) return;

    if (!validatePaths()) {
      alert('Папка-источник и папка назначения не могут совпадать!');
      return;
    }

    const selected = getSelected();
    if (!selected.length) { alert('Выберите страницы!'); return; }

    const target = getTarget();
    if (!target) { alert('Укажите папку назначения!'); return; }

    const headers = await ensureHeaders();
    if (!headers?.orgId) {
      alert('Не удалось получить данные организации.\nКликните по любой странице в дереве и нажмите "Сканировать" снова.');
      return;
    }

    isRunning        = true;
    btnMove.disabled = true;
    btnScan.disabled = true;
    reloadBar.style.display = 'none';
    setProgress(0);

    let okCount = 0, failCount = 0;

    for (let i = 0; i < selected.length; i++) {
      const page    = selected[i];
      const origIdx = foundPages.indexOf(page);

      setStatus(origIdx, 'running', '↻', 'Переносится...');
      log(`Переношу: ${page.title}`, 'info');

      try {
        const tab = await getTab();
        if (!tab) throw new Error('Нет активной вкладки');

        // Первая попытка с copyInheritedAccess = false
        let moveResults = await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          func  : callMoveAPI,
          args  : [
            [{ source: page.source, target: target + '/' + page.slug }],
            headers.orgId,
            headers.collabOrgId,
            headers.csrfToken,
            false
          ]
        });

        let res = moveResults[0]?.result;

        // Если сервер сигнализирует о различии прав — спрашиваем пользователя
        const isInheritanceIssue =
          !res?.ok &&
          (res?.status === 400 || res?.status === 409) &&
          JSON.stringify(res?.data).toLowerCase().includes('inherit');

        if (isInheritanceIssue) {
          log(`Различие прав доступа для: ${page.title}`, 'warn');
          const copy = await askInheritance(page.title);

          moveResults = await chrome.scripting.executeScript({
            target: { tabId: tab.id },
            func  : callMoveAPI,
            args  : [
              [{ source: page.source, target: target + '/' + page.slug }],
              headers.orgId,
              headers.collabOrgId,
              headers.csrfToken,
              copy
            ]
          });
          res = moveResults[0]?.result;
        }

        if (!res?.ok) {
          throw new Error(
            res?.status === 403 ? 'Нет прав или устарел токен — обновите страницу Wiki и повторите' :
            res?.status === 0   ? 'Таймаут запроса' :
            `Ошибка сервера (${res?.status})`
          );
        }

        const taskId = res.data?.operation?.id || res.data?.taskId || res.data?.id;

        if (taskId) {
          setStatus(origIdx, 'running', '↻', 'Ожидание...');

          const pollResults = await chrome.scripting.executeScript({
            target: { tabId: tab.id },
            func  : pollMoveStatus,
            args  : [taskId, headers.orgId, headers.collabOrgId, headers.csrfToken]
          });

          const poll = pollResults[0]?.result;
          if (!poll?.ok) throw new Error(poll?.error || 'Операция завершилась с ошибкой');

          setStatus(origIdx, 'success', '✓', 'Перенесено');
          log(`Перенесено: ${page.title} (${poll.pages ?? '?'} стр.)`, 'success');
        } else {
          setStatus(origIdx, 'success', '✓', 'Перенесено');
          log(`Перенесено: ${page.title}`, 'success');
        }

        okCount++;

      } catch (err) {
        setStatus(origIdx, 'error', '✗', err.message);
        log(`Ошибка: ${page.title} — ${err.message}`, 'error');
        failCount++;
        if (err.message.includes('токен')) {
          capturedHeaders = null;
          await ensureHeaders();
        }
      }

      setProgress(Math.round(((i + 1) / selected.length) * 100));
      if (i < selected.length - 1) await sleep(500);
    }

    log(
      `Готово: ${okCount} перенесено${failCount > 0 ? `, ${failCount} ошибок` : ''}`,
      failCount === 0 ? 'success' : 'warn'
    );

    isRunning        = false;
    btnMove.disabled = false;
    btnScan.disabled = false;

    if (okCount > 0) reloadBar.style.display = 'flex';
  }

  function saveSettings() {
    chrome.storage.local.set({ sourcePath: getSource(), targetPath: getTarget() });
  }

  ['source-path', 'target-path'].forEach(id => {
    document.getElementById(id).addEventListener('change', saveSettings);
  });

  chrome.storage.local.get(['sourcePath', 'targetPath'], data => {
    if (data.sourcePath) {
      sourceInput.value = data.sourcePath;
      updateHint('source-path', sourceHint);
    }
    if (data.targetPath) {
      targetInput.value = data.targetPath;
      updateHint('target-path', targetHint);
    }
  });

  btnScan.addEventListener('click', scanPages);
  btnMove.addEventListener('click', movePages);
  btnSelectAll.addEventListener('click', toggleAll);

});
