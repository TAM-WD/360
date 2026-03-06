(function () {
  'use strict';

  console.log('🎥 Телемост Авто-Запись v2.5 FINAL (enhanced)');

  const CONFIG = {
    CHECK_INTERVAL: 2000,
    MIN_PARTICIPANTS: 2,
    CLICK_DELAY: 1500,
    WAIT_FOR_MENU: 4000, // оставим, но ниже для записи используем более быстрый опрос
    STATE_RESET_ON_RELOAD: true,

    // Улучшения
    DEBUG: false,               // true = подробные info/success логи
    ATTEMPT_COOLDOWN: 12000,    // не пытаться запускать запись/конспект чаще, чем раз в N мс
    MENU_FIND_RETRY_COUNT: 12,  // 12 * 250мс ≈ 3 сек
    MENU_FIND_RETRY_STEP: 250,

    // Тихие режимы
    SILENT_NO_TRANSCRIPTION_RIGHTS: true,
    SILENT_NO_RECORDING_RIGHTS: true,
    SILENT_SOFT_WARNINGS: true,

    // Подстраховка: сколько раз допускаем "пункта записи нет" прежде чем решить что прав нет
    RECORDING_MISSING_ITEM_MAX_ATTEMPTS: 1,

    // Пост-проверка старта записи (кликнули, но вдруг не стартануло)
    RECORDING_POSTCHECK_RETRIES: 10, // 10 * 500мс = 5 сек
    RECORDING_POSTCHECK_STEP: 500
  };

  const SESSION_ID = Date.now();
  const STATE_KEY = `telemostState_${window.location.pathname}`;

  let state = {
    recordingStarted: false,
    transcriptionStarted: false,
    isProcessing: false,
    lastParticipantCount: 0,
    sessionId: SESSION_ID,

    // НИКОГДА не сбрасываются
    recordingEverStarted: false,
    transcriptionEverStarted: false,

    // чтобы предупреждение "конспект недоступен" было только 1 раз за встречу
    transcriptionUnavailableNotified: false,

    // для записи
    recordingUnavailableNotified: false,
    recordingMenuMissingAttempts: 0
  };

  // runtime (не сохраняем)
  let lastAttemptAt = 0;
  let lastSavedState = '';
  let observerAttached = false;
  let stopped = false;

  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

  const debounce = (fn, ms) => {
    let t = null;
    return (...args) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...args), ms);
    };
  };

  const log = {
    info: (msg, ...args) => CONFIG.DEBUG && console.log(`ℹ️ [Телемост]:`, msg, ...args),
    success: (msg, ...args) => CONFIG.DEBUG && console.log(`✅ [Телемост]:`, msg, ...args),
    warn: (msg, ...args) => console.warn(`⚠️ [Телемост]:`, msg, ...args),
    error: (msg, ...args) => console.error(`❌ [Телемост]:`, msg, ...args)
  };

  function softWarn(message, ...args) {
    if (!CONFIG.SILENT_SOFT_WARNINGS) {
      log.warn(message, ...args);
    }
  }

  // Проверка "нельзя кликать / disabled" (на всякий случай)
  function isDisabledElement(el) {
    if (!el) return true;

    try {
      if (el.getAttribute && el.getAttribute('aria-disabled') === 'true') return true;
      if (el.disabled === true) return true;

      const disabledParent = el.closest && el.closest('[aria-disabled="true"], [disabled]');
      if (disabledParent) return true;

      const cs = window.getComputedStyle(el);
      if (cs) {
        if (cs.pointerEvents === 'none') return true;
        if (cs.visibility === 'hidden' || cs.display === 'none') return true;
        if (parseFloat(cs.opacity) === 0) return true;
      }

      const cls = (el.className || '').toString().toLowerCase();
      if (cls.includes('disabled') || cls.includes('inactive')) return true;

      return false;
    } catch {
      return false;
    }
  }

  // ========================================
  // ОВЕРЛЕЙ (стили добавляются 1 раз)
  // ========================================

  function ensureOverlayStyles() {
    if (document.getElementById('telemost-overlay-styles')) return;

    const style = document.createElement('style');
    style.id = 'telemost-overlay-styles';
    style.textContent = `
      @keyframes telemostFadeIn { from { opacity: 0; } to { opacity: 1; } }
      @keyframes telemostScaleIn { from { transform: scale(0.8); opacity: 0; } to { transform: scale(1); opacity: 1; } }
      @keyframes telemostPulse { 0%, 100% { transform: scale(1); } 50% { transform: scale(1.1); } }
    `;
    document.head.appendChild(style);
  }

  // options:
  // - opaque: true => полностью скрывает то, что под оверлеем (меню "Ещё" не будет видно вообще)
  function showOverlay(message = 'Запускаю...', icon = '🎥', options = {}) {
    ensureOverlayStyles();

    const { opaque = false } = options;

    const oldOverlay = document.getElementById('telemost-overlay');
    if (oldOverlay) oldOverlay.remove();

    const overlay = document.createElement('div');
    overlay.id = 'telemost-overlay';
    overlay.style.cssText = `
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.92);
  z-index: 999999;
  display: flex;
  align-items: center;
  justify-content: center;
  backdrop-filter: none;
  animation: telemostFadeIn 0.2s ease-out;
`;


    const messageBox = document.createElement('div');
    messageBox.style.cssText = `
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  padding: 40px 40px;
  border-radius: 20px;
  font-size: 22px;
  font-weight: bold;
  box-shadow: 0 20px 60px rgba(0,0,0,0.4);
  text-align: center;
  animation: telemostScaleIn 0.2s ease-out;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;

  width: 400px;
  max-width: calc(100vw - 40px);
  min-height: 260px;

  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

    messageBox.innerHTML = `
  <div data-telemost="icon" style="height: 84px; display:flex; align-items:center; justify-content:center; font-size: 64px; margin-bottom: 12px; animation: telemostPulse 1.5s infinite;">
    ${icon}
  </div>
  <div data-telemost="message" style="min-height: 64px; display:flex; align-items:center; justify-content:center; font-size: 24px; margin-bottom: 10px;">
    ${message}
  </div>
  <div data-telemost="sub" style="height: 22px; font-size: 16px; opacity: 0.9; font-weight: normal;">
    Пожалуйста, подождите...
  </div>
`;


    overlay.appendChild(messageBox);
    document.body.appendChild(overlay);

    return overlay;
  }

  function updateOverlay(overlay, message, icon, subText) {
    if (!overlay) return;

    const iconEl = overlay.querySelector('[data-telemost="icon"]');
    const msgEl = overlay.querySelector('[data-telemost="message"]');
    const subEl = overlay.querySelector('[data-telemost="sub"]');

    if (iconEl && icon != null) iconEl.textContent = icon;
    if (msgEl && message != null) msgEl.textContent = message;

    if (subEl) {
      if (subText === undefined) return; // не трогаем
      if (subEl) {
  if (subText === undefined) return; // не трогаем
  if (subText === null) {
    // место остаётся, но текста не видно (размер оверлея не меняется)
    subEl.style.visibility = 'hidden';
    subEl.textContent = '...';
  } else {
    subEl.style.visibility = 'visible';
    subEl.textContent = subText;
  }
}

      else {
        subEl.style.display = '';
        subEl.textContent = subText;
      }
    }
  }

  function hideOverlay(overlay) {
    if (overlay && overlay.parentNode) {
      overlay.style.opacity = '0';
      overlay.style.transition = 'opacity 0.2s';
      setTimeout(() => overlay.remove(), 200);
    }
  }

  // ========================================
  // СОСТОЯНИЕ (умное сохранение)
  // ========================================

  function saveStateSmart() {
    try {
      const serialized = JSON.stringify(state);
      if (serialized === lastSavedState) return;
      lastSavedState = serialized;
      localStorage.setItem(STATE_KEY, serialized);
    } catch (e) {
      log.error('Ошибка сохранения:', e);
    }
  }

  function loadState() {
    try {
      const saved = localStorage.getItem(STATE_KEY);
      if (saved && !CONFIG.STATE_RESET_ON_RELOAD) {
        state = { ...state, ...JSON.parse(saved), sessionId: SESSION_ID };
        log.info('📂 Состояние загружено');
      } else {
        log.info('🆕 Новая сессия');
      }
      lastSavedState = JSON.stringify(state);
    } catch (e) {
      log.error('Ошибка загрузки:', e);
    }
  }

  // ========================================
  // ПОИСК ЭЛЕМЕНТОВ
  // ========================================

  function getParticipantCount() {
    try {
      const btn = document.querySelector('button[aria-label="Участники"]');
      if (!btn) return null;

      const badge = btn.querySelector('[class*="badge"]');
      if (badge) {
        const count = parseInt(badge.textContent.trim(), 10);
        if (!isNaN(count)) return count;
      }

      const text = btn.textContent.trim();
      const match = text.match(/(\d+)/);
      if (match) {
        const count = parseInt(match[1], 10);
        if (!isNaN(count)) return count;
      }

      return null;
    } catch {
      return null;
    }
  }

  function findTranscriptionButton() {
    try {
      let btn = document.querySelector('button[aria-label="Конспектировать с Алисой Про"]');
      if (btn) {
        const ariaDisabled = btn.getAttribute('aria-disabled');
        const disabled = btn.disabled;
        if (ariaDisabled === 'true' || disabled) return null;
        return btn;
      }

      const buttons = document.querySelectorAll('button');
      for (const button of buttons) {
        const text = button.textContent || '';
        const ariaLabel = button.getAttribute('aria-label') || '';

        if (
          text.includes('Конспект') ||
          text.includes('Алис') ||
          ariaLabel.includes('Конспект') ||
          ariaLabel.includes('Алис')
        ) {
          const ariaDisabled = button.getAttribute('aria-disabled');
          const disabled = button.disabled;
          if (ariaDisabled === 'true' || disabled) return null;
          return button;
        }
      }

      return null;
    } catch {
      return null;
    }
  }

  function findMoreMenuButton() {
    try {
      const toolbar = document.querySelector('[class*="toolbar"]');
      if (!toolbar) return null;

      const buttons = toolbar.querySelectorAll('button');

      for (const btn of buttons) {
        const ariaLabel = btn.getAttribute('aria-label');
        if (ariaLabel && (ariaLabel.includes('Ещё') || ariaLabel.includes('Еще'))) return btn;
      }

      const emptyButtons = Array.from(buttons).filter(btn => !btn.textContent.trim());
      if (emptyButtons.length >= 2) return emptyButtons[emptyButtons.length - 2];

      return null;
    } catch {
      return null;
    }
  }

  function findYandexDiskRecordButton() {
    try {
      let element = document.querySelector('div.toolbarOption_iw2uN');
      if (element && element.textContent.trim() === 'Записать на Яндекс Диск') return element;

      element = document.querySelector('div.option_25W9Q[title="Записать на Яндекс Диск"]');
      if (element) return element;

      const allDivs = document.querySelectorAll('div, span');
      for (const div of allDivs) {
        if (div.textContent.trim() === 'Записать на Яндекс Диск' && div.offsetParent !== null) {
          return div;
        }
      }

      const spans = document.querySelectorAll('span.content_47PYN');
      for (const span of spans) {
        if (span.textContent.includes('Записать на Яндекс Диск')) return span;
      }

      return null;
    } catch {
      return null;
    }
  }

  function isRecordingActive(bodyText) {
    try {
      const text = bodyText || document.body.innerText || '';

      if (text.includes('Остановить запись на Яндекс Диск')) return true;
      if (text.toLowerCase().includes('остановить запись')) return true;

      const toolbar = document.querySelector('[class*="toolbar"]');
      if (toolbar) {
        const buttons = toolbar.querySelectorAll('button');
        for (const btn of buttons) {
          const t = btn.textContent.trim();
          if (
            ((t.includes('Запись') && t.includes('Яндекс Диск')) || t === 'Запись Яндекс Диск') &&
            btn.offsetParent !== null
          ) {
            return true;
          }
        }
      }

      return false;
    } catch {
      return false;
    }
  }

  function isTranscriptionActive(bodyText) {
    try {
      const text = bodyText || document.body.innerText || '';

      if (text.includes('Идёт конспект')) return true;
      if (text.includes('Остановить конспект')) return true;

      const btn = findTranscriptionButton();
      if (btn) {
        const ariaPressed = btn.getAttribute('aria-pressed');
        const ariaChecked = btn.getAttribute('aria-checked');
        if (ariaPressed === 'true' || ariaChecked === 'true') return true;
      }

      return false;
    } catch {
      return false;
    }
  }

  // ========================================
  // ДЕЙСТВИЯ
  // ========================================

  async function clickElement(element, description) {
    if (!element) {
      log.error(`Не могу кликнуть: ${description}`);
      return false;
    }

    try {
      log.info(`🖱️ Клик: ${description}`);

      element.scrollIntoView({ behavior: 'smooth', block: 'center' });
      await sleep(300);

      element.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
      await sleep(50);
      element.click();
      await sleep(50);
      element.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));

      await sleep(CONFIG.CLICK_DELAY);

      log.success(`✅ Клик: ${description}`);
      return true;
    } catch (e) {
      log.error('Ошибка клика:', e);
      return false;
    }
  }

  async function startRecording() {
    log.info('🎬 startRecording');

    if (state.recordingStarted) {
      log.info('⏭️ Запись уже помечена');
      return true;
    }

    log.info('⏳ Задержка для стабилизации...');
    await sleep(1000);

    const bodyText = document.body.innerText || '';
    if (isRecordingActive(bodyText)) {
      log.info('🔴 Запись уже активна, пропускаю (без оверлея)');
      state.recordingStarted = true;
      state.recordingEverStarted = true;
      state.recordingMenuMissingAttempts = 0;
      saveStateSmart();
      return true;
    }

    // Один оверлей на всю операцию (без второго и третьего)
    const overlay = showOverlay('Проверяю запись на Яндекс Диск', '🎥', { opaque: true });

    try {
      const moreButton = findMoreMenuButton();
      if (!moreButton) {
        softWarn('⚠️ Кнопка "Ещё" не найдена (повторю попытку позже)');
        hideOverlay(overlay);
        return false;
      }

      if (!await clickElement(moreButton, 'Меню "Ещё"')) {
        softWarn('⚠️ Не удалось нажать "Ещё" (повторю попытку позже)');
        hideOverlay(overlay);
        return false;
      }

      await sleep(250);

      let recordButton = null;
      for (let i = 0; i < CONFIG.MENU_FIND_RETRY_COUNT; i++) {
        recordButton = findYandexDiskRecordButton();
        if (recordButton) break;
        await sleep(CONFIG.MENU_FIND_RETRY_STEP);
      }

      if (!recordButton) {
        // Закрываем меню
        document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27, bubbles: true }));
        await sleep(200);
        document.body.click();

        // Если запись уже активна кем-то другим — просто отметим
        if (isRecordingActive()) {
          state.recordingStarted = true;
          state.recordingEverStarted = true;
          state.recordingMenuMissingAttempts = 0;
          saveStateSmart();

          updateOverlay(overlay, 'Запись уже идёт', '🔴', null);
          setTimeout(() => hideOverlay(overlay), 700);
          return true;
        }

        // 1 проверка: пункта нет => считаем недоступно и больше не пытаемся
state.recordingStarted = true;
state.recordingEverStarted = true;
state.recordingUnavailableNotified = true;
state.recordingMenuMissingAttempts = 0;
saveStateSmart();

hideOverlay(overlay);
return false;

      }

      // Нашли пункт — сбрасываем счетчик
      state.recordingMenuMissingAttempts = 0;
      saveStateSmart();

      if (isDisabledElement(recordButton)) {
        document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27, bubbles: true }));
        await sleep(200);
        document.body.click();

        if (!state.recordingUnavailableNotified) {
          if (!CONFIG.SILENT_NO_RECORDING_RIGHTS) {
            log.warn('⚠️ Запись на Яндекс Диск недоступна (пункт disabled)');
          }
          state.recordingUnavailableNotified = true;
        }

        state.recordingStarted = true;
        state.recordingEverStarted = true;
        saveStateSmart();

        hideOverlay(overlay);
        return false;
      }

      // НЕ создаём второй оверлей — просто обновляем этот же
      updateOverlay(overlay, 'Запускаю запись на Яндекс Диск', '🎥');

      if (!await clickElement(recordButton, 'Записать на Яндекс Диск')) {
        softWarn('⚠️ Не удалось нажать "Записать на Яндекс Диск" (повторю попытку позже)');
        document.body.click();
        hideOverlay(overlay);
        return false;
      }

      // Пост-проверка что запись реально стартовала
      let started = false;
      for (let i = 0; i < CONFIG.RECORDING_POSTCHECK_RETRIES; i++) {
        await sleep(CONFIG.RECORDING_POSTCHECK_STEP);
        if (isRecordingActive()) {
          started = true;
          break;
        }
      }

      if (!started) {
        if (!state.recordingUnavailableNotified) {
          if (!CONFIG.SILENT_NO_RECORDING_RIGHTS) {
            log.warn('⚠️ Не удалось запустить запись (возможно, нет прав или требуется подтверждение)');
          }
          state.recordingUnavailableNotified = true;
        }

        state.recordingStarted = true;
        state.recordingEverStarted = true;
        saveStateSmart();

        document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27, bubbles: true }));
        await sleep(200);
        document.body.click();
        hideOverlay(overlay);
        return false;
      }

      state.recordingStarted = true;
      state.recordingEverStarted = true;
      saveStateSmart();

      // НЕ создаём successOverlay — обновляем тот же
      updateOverlay(overlay, 'Запись запущена!', '✅', null);
      setTimeout(() => hideOverlay(overlay), 1200);

      log.success('🎉 ЗАПИСЬ ЗАПУЩЕНА!');
      return true;

    } catch (e) {
      log.error('Ошибка:', e);
      document.body.click();
      hideOverlay(overlay);
      return false;
    }
  }

  async function startTranscription() {
    log.info('📝 startTranscription');

    if (state.transcriptionStarted) {
      log.info('⏭️ Конспект помечен');
      return true;
    }

    await sleep(1000);

    const bodyText = document.body.innerText || '';
    if (isTranscriptionActive(bodyText)) {
      log.info('📝 Конспект уже активен, пропускаю (без оверлея)');
      state.transcriptionStarted = true;
      state.transcriptionEverStarted = true;
      saveStateSmart();
      return true;
    }

    const button = findTranscriptionButton();
    if (!button) {
      if (!state.transcriptionUnavailableNotified) {
        if (!CONFIG.SILENT_NO_TRANSCRIPTION_RIGHTS) {
          log.warn('⚠️ Конспектирование недоступно (нет прав организатора)');
        }
        state.transcriptionUnavailableNotified = true;
      }

      state.transcriptionStarted = true;
      state.transcriptionEverStarted = true;
      saveStateSmart();
      return false;
    }

    const overlay = showOverlay('Запускаю конспектирование', '📝');

    try {
      log.info('▶️ Запуск конспектирования...');

      if (!await clickElement(button, 'Конспектирование')) {
        hideOverlay(overlay);
        state.transcriptionStarted = true;
        state.transcriptionEverStarted = true;
        saveStateSmart();
        return false;
      }

      state.transcriptionStarted = true;
      state.transcriptionEverStarted = true;
      saveStateSmart();

      // НЕ создаём successOverlay — обновляем тот же
      updateOverlay(overlay, 'Конспектирование запущено!', '✅', null);
      setTimeout(() => hideOverlay(overlay), 1200);

      log.success('🎉 КОНСПЕКТИРОВАНИЕ ЗАПУЩЕНО!');
      return true;

    } catch (e) {
      log.error('Ошибка:', e);
      hideOverlay(overlay);

      state.transcriptionStarted = true;
      state.transcriptionEverStarted = true;
      saveStateSmart();
      return false;
    }
  }

  // ========================================
  // MutationObserver (ускоряет реакцию)
  // ========================================

  function tryAttachParticipantsObserver(checkFn) {
    if (observerAttached) return;

    const participantsBtn = document.querySelector('button[aria-label="Участники"]');
    if (!participantsBtn) return;

    const mo = new MutationObserver(() => checkFn());
    mo.observe(participantsBtn, { childList: true, subtree: true, characterData: true });
    observerAttached = true;

    log.info('👀 MutationObserver подключен к кнопке "Участники"');
  }

  // ========================================
  // ОСНОВНАЯ ЛОГИКА
  // ========================================

  async function checkAndActivate() {
    if (state.isProcessing) return;

    const count = getParticipantCount();
    if (count === null) return;

    if (count !== state.lastParticipantCount) {
      log.info(`👥 Участников: ${count}`);
      state.lastParticipantCount = count;

      if (count < CONFIG.MIN_PARTICIPANTS) {
        log.info('📉 Участников стало меньше 2, но запись/конспект продолжаются');
        return;
      }
    }

    if (count < CONFIG.MIN_PARTICIPANTS) return;

    if (state.recordingEverStarted && state.transcriptionEverStarted) return;
    if (state.recordingStarted && state.transcriptionStarted) return;

    const bodyText = document.body.innerText || '';

    if (!state.recordingStarted && isRecordingActive(bodyText)) {
      log.success('🔴 Запись уже идёт!');
      state.recordingStarted = true;
      state.recordingEverStarted = true;
      saveStateSmart();
    }

    if (!state.transcriptionStarted && isTranscriptionActive(bodyText)) {
      log.success('📝 Конспект уже идёт!');
      state.transcriptionStarted = true;
      state.transcriptionEverStarted = true;
      saveStateSmart();
    }

    if (state.recordingStarted && state.transcriptionStarted) return;

    const needAttempt =
      (!state.recordingStarted && !isRecordingActive(bodyText)) ||
      (!state.transcriptionStarted && !isTranscriptionActive(bodyText));

    if (!needAttempt) return;

    const now = Date.now();
    if (now - lastAttemptAt < CONFIG.ATTEMPT_COOLDOWN) return;
    lastAttemptAt = now;

    state.isProcessing = true;

    try {
      log.info(`🚀 АКТИВАЦИЯ! Участников: ${count}`);

      if (!state.recordingStarted && !isRecordingActive(bodyText)) {
        await startRecording();
      }

      await sleep(2000);

      if (!state.transcriptionStarted && !isTranscriptionActive(document.body.innerText || '')) {
        await startTranscription();
      }
    } catch (e) {
      log.error('❌ Ошибка:', e);
    } finally {
      state.isProcessing = false;
      saveStateSmart();
    }
  }

  // ========================================
  // ИНИЦИАЛИЗАЦИЯ (умный цикл вместо setInterval)
  // ========================================

  function init() {
    log.info('🚀 Инициализация v2.5 FINAL...');

    loadState();

    const isMeetingPage =
      window.location.hostname.includes('telemost') &&
      window.location.pathname.includes('/j/');

    if (!isMeetingPage) {
  if (CONFIG.DEBUG) log.warn('⚠️ Не страница встречи');
  return;
}


    log.success('✅ Страница встречи');

    const debouncedCheck = debounce(checkAndActivate, 400);

    tryAttachParticipantsObserver(debouncedCheck);
    const attachObserverRetry = debounce(() => tryAttachParticipantsObserver(debouncedCheck), 1000);

    setTimeout(() => {
      attachObserverRetry();
      checkAndActivate();
    }, 3000);

    async function loop() {
      if (stopped) return;
      try {
        attachObserverRetry();
        await checkAndActivate();
      } finally {
        setTimeout(loop, CONFIG.CHECK_INTERVAL);
      }
    }

    loop();
    log.success('✅✅✅ РАСШИРЕНИЕ РАБОТАЕТ!');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  window.addEventListener('beforeunload', () => {
    stopped = true;
  });

})();
