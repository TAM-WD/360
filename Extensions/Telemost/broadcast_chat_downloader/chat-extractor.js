(function (root) {
  'use strict';

  async function run(options = {}) {
    const warnings = new Set();
    const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
    const trim = value => (value || '').trim();
    const visible = node => node && node.getClientRects().length > 0 &&
      getComputedStyle(node).visibility !== 'hidden';
    const roots = Array.from(document.querySelectorAll('.yamb-conversation__content'))
      .filter(visible);
    if (!roots.length) return { detected: false, error: 'Откройте переписку и дождитесь загрузки сообщений.' };
    if (roots.length > 1) return { detected: false, error: 'Открыто несколько переписок. Закройте тред или дополнительную панель и повторите выгрузку.' };
    const content = roots[0];
    const conversation = content.closest('.yamb-conversation') || content.parentElement;
    // In the unified Telemost, this attribute belongs to ScrollContainer, not InfiniteList.
    const legacy = conversation.querySelector('.ui-InfiniteList-ScrollContainer[data-scroll-container="true"]') ||
      conversation.querySelector('.ui-InfiniteList[data-scroll-container="true"]');
    let scroller = legacy;
    if (!scroller) {
      for (let node = content; node && node !== document.body; node = node.parentElement) {
        if (node.matches('.ui-virtual-list__scroller, .ui-scroll-area__container') ||
            /auto|scroll/.test(getComputedStyle(node).overflowY)) {
          scroller = node;
          break;
        }
      }
    }
    if (!scroller) return { detected: false, error: 'Не найден контейнер прокрутки переписки.' };
    const titleNode = document.querySelector('.yamb-chat-header__title, .yamb-conversation-header__title');
    const chatTitle = trim(titleNode && titleNode.textContent) || document.title.replace(/\s*[—–]\s*\d+.*$/, '');
    if (options.mode === 'inspect') return { detected: true, chatTitle, adapter: legacy ? 'infinite-list' : 'native-scroll' };
    if (root.__chatDownloaderRunning) return { error: 'В этой вкладке уже идёт выгрузка.' };
    root.__chatDownloaderRunning = true;

    const startedAt = Date.now();
    const originalURL = location.href;
    const delayMs = Math.max(20, options.delayMs || 300);
    const stableRounds = Math.max(2, options.stableRounds || 12);
    const maxDurationMs = options.maxDurationMs || 10 * 60 * 1000;
    const maxSteps = options.maxSteps || 2500;
    const messages = new Map();
    const anonymousNodes = new WeakMap();
    const seenPositions = new Set();
    let knownTotal = 0;
    let anonymousIndex = 0;
    let steps = 0;
    let reason = 'stable-boundaries';
    let startReached = false;
    let endReached = false;

    const articles = () => Array.from(content.querySelectorAll('article.message'));
    function messageId(article) {
      const explicit = article.getAttribute('data-message-id');
      if (explicit) return explicit;
      const labels = trim(article.getAttribute('aria-labelledby')).split(/\s+/);
      // The current Messenger labels are <message-id>_h/_fr/_c/_i/_fo.
      const bases = labels.map(label => /^(\d+)_(?:h|fr|c|i|fo)$/.exec(label)).filter(Boolean).map(match => match[1]);
      if (bases.length && bases.every(base => base === bases[0])) return bases[0];
      if (article.id) return article.id;
      const position = article.getAttribute('aria-posinset');
      if (position && Number(position) > 0) {
        warnings.add('Идентификаторы части сообщений восстановлены по позиции в истории.');
        return `position-${position}`;
      }
      warnings.add('У части сообщений нет устойчивого ID; возможны повторы после перерисовки страницы.');
      if (!anonymousNodes.has(article)) anonymousNodes.set(article, `unidentified-${++anonymousIndex}`);
      return anonymousNodes.get(article);
    }
    function renderedText(node) {
      if (!node) return '';
      // innerText preserves paragraphs and line breaks in copyable rich text.
      return trim(node.innerText || node.textContent);
    }
    function parseArticle(article) {
      const row = article.querySelector('.yamb-message-row');
      const system = article.querySelector('.yamb-message-system');
      // A date separator can precede a NORMAL message within the same article.
      if (!row && system && /^(?:сегодня|вчера|today|yesterday|\d{1,2}\s+[а-яa-z]+(?:\s+\d{4})?)$/i.test(renderedText(system))) return null;
      const id = messageId(article);
      const own = Boolean(article.querySelector('.yamb-message-row_own'));
      const senderNode = article.querySelector('.yamb-message-user');
      const sender = trim(senderNode && senderNode.getAttribute('aria-label')) ||
        renderedText(article.querySelector('.yamb-message-user__name')) || '';
      const reply = article.querySelector('.yamb-message-reply');
      const textNodes = Array.from(article.querySelectorAll('[data-copyable="true"]'))
        .filter(node => !node.closest('.yamb-message-reply, .yamb-message-system'));
      const outerTextNodes = textNodes.filter(node => !textNodes.some(other => other !== node && other.contains(node)));
      let text = outerTextNodes.map(renderedText).filter(Boolean).join('\n');
      if (!text) {
        const textBlock = article.querySelector('.yamb-message-text');
        if (textBlock) {
          // Older variants put the clock inside this wrapper.
          const plain = textBlock.querySelector('.text');
          text = renderedText(plain) || Array.from(textBlock.childNodes)
            .filter(node => node.nodeType === 3).map(node => trim(node.textContent)).join('\n');
        }
      }
      const attachments = [];
      article.querySelectorAll('.yamb-message-file').forEach(file => {
        if (file.closest('.yamb-message-reply')) return;
        const name = renderedText(file.querySelector('.yamb-message-file__name')) || 'Файл';
        const size = renderedText(file.querySelector('.yamb-message-file__size'));
        attachments.push(size ? `${name} (${size})` : name);
      });
      const attachmentTypes = [
        ['.yamb-message-image', 'Изображение'],
        ['.yamb-gallery-image', 'Изображение'],
        ['.yamb-message-video', 'Видео'],
        ['.yamb-message-voice', 'Голосовое сообщение'],
        ['.yamb-message-audio', 'Аудио'],
        ['.yamb-message-sticker', 'Стикер']
      ];
      attachmentTypes.forEach(([selector, label]) => {
        article.querySelectorAll(selector).forEach(node => {
          if (!node.closest('.yamb-message-reply')) attachments.push(label);
        });
      });
      // Links to authenticated media are deliberately not stored; only visible descriptions.
      const timeNode = article.querySelector('.yamb-message-info__time');
      const time = trim(timeNode && (timeNode.getAttribute('aria-label') || timeNode.textContent));
      const datetime = timeNode && timeNode.getAttribute('datetime');
      const date = datetime ? datetime.slice(0, 10) : (time.match(/^\d{1,2}:\d{2}\s+(.+)$/) || [])[1] ||
        (row && system ? renderedText(system) : '');
      const questionText = renderedText(reply && reply.querySelector('.yamb-message-reply__description'));
      const questionAuthor = renderedText(reply && reply.querySelector('.yamb-message-reply__title'));
      const replyToId = reply && (reply.getAttribute('data-reply-to-id') || reply.getAttribute('data-message-id')) || '';
      if (!row && system) text = renderedText(system);
      if (!text && !attachments.length && row) {
        text = '[Сообщение без доступного текста]';
        warnings.add('Встречены сообщения, содержимое которых интерфейс не предоставил в текстовом виде.');
      }
      if (!text && !attachments.length) return null;
      return { id, time, date, sender, isOwnMessage: own, message: text, replyToId,
        questionText, questionAuthor, attachments, type: !row && system ? 'Системное' : reply ? 'Ответ' : 'Сообщение',
        order: Number(article.getAttribute('aria-posinset')) || 0 };
    }
    function collect() {
      for (const article of articles()) {
        const position = Number(article.getAttribute('aria-posinset'));
        if (Number.isInteger(position) && position > 0) seenPositions.add(position);
        const total = Number(article.getAttribute('aria-setsize'));
        if (Number.isInteger(total) && total > 0) knownTotal = Math.max(knownTotal, total);
        const message = parseArticle(article);
        if (!message) continue;
        const previous = messages.get(message.id);
        if (!previous) messages.set(message.id, message);
        else messages.set(message.id, { ...message, sender: message.sender || previous.sender,
          date: message.date || previous.date });
      }
    }
    function checkState() {
      if (location.href !== originalURL || !content.isConnected || !scroller.isConnected || !visible(content)) {
        throw new Error('Во время выгрузки чат изменился. Откройте нужную переписку и повторите выгрузку.');
      }
      if (Date.now() - startedAt >= maxDurationMs || steps >= maxSteps) {
        reason = 'limit';
        return false;
      }
      return true;
    }
    function snapshot() {
      const list = articles();
      const transformed = conversation.querySelector('.ui-InfiniteList-Container');
      return [list.map(messageId).join(','), scroller.scrollTop, scroller.scrollHeight,
        transformed ? getComputedStyle(transformed).transform : '',
        Boolean(conversation.querySelector('[aria-busy="true"], [role="progressbar"]'))].join('|');
    }
    function atBoundary(direction) {
      const list = articles();
      if (!list.length) return false;
      const firstPosition = Number(list[0].getAttribute('aria-posinset'));
      const last = list[list.length - 1];
      const lastPosition = Number(last.getAttribute('aria-posinset'));
      const total = Number(last.getAttribute('aria-setsize'));
      if (direction < 0 && firstPosition === 1) return true;
      if (direction > 0 && total > 0 && lastPosition >= total) return true;
      if (legacy) return false;
      if (direction < 0) return scroller.scrollTop <= 1 && (!firstPosition || firstPosition === 1);
      return scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 1;
    }
    async function traverse(direction) {
      let stable = 0;
      let previous = snapshot();
      while (stable < stableRounds && checkState()) {
        collect();
        const delta = direction * Math.max(100, Math.floor(scroller.clientHeight * 0.65));
        if (legacy) scroller.dispatchEvent(new WheelEvent('wheel', { deltaY: delta, deltaMode: 0, bubbles: true, cancelable: true }));
        else scroller.scrollBy({ top: delta, behavior: 'instant' });
        steps++;
        await sleep(delayMs);
        if (!checkState()) break;
        collect();
        const current = snapshot();
        const loading = Boolean(conversation.querySelector('[aria-busy="true"], [role="progressbar"]'));
        stable = !loading && current === previous ? stable + 1 : 0;
        previous = current;
      }
      return reason !== 'limit' && stable >= stableRounds && atBoundary(direction);
    }
    try {
      collect();
      startReached = await traverse(-1);
      if (reason !== 'limit') endReached = await traverse(1);
      collect();
      const result = Array.from(messages.values());
      result.sort((a, b) => {
        if (a.order && b.order) return a.order - b.order;
        if (/^\d+$/.test(a.id) && /^\d+$/.test(b.id)) return a.id.length - b.id.length || a.id.localeCompare(b.id);
        return 0;
      });
      result.forEach((message, index) => { message.order = index; });
      const expectedPositions = knownTotal || Array.from(seenPositions).reduce((highest, position) => Math.max(highest, position), 0);
      const hasGaps = expectedPositions > 0 && Array.from(seenPositions).filter(position => position <= expectedPositions).length < expectedPositions;
      const unstableIds = result.some(message => message.id.startsWith('unidentified-'));
      if (hasGaps) {
        if (reason !== 'limit') reason = 'gaps';
        warnings.add('В истории обнаружены пропуски позиций; интерфейс мог перескочить часть сообщений при прокрутке.');
      }
      const coverage = { startReached, endReached, complete: startReached && endReached && reason !== 'limit' && !hasGaps && !unstableIds, reason };
      if (!coverage.complete) warnings.add(reason === 'limit'
        ? 'Достигнут предел времени или прокруток. Сохранена только собранная часть истории.'
        : 'Сохранена доступная история. Интерфейс не подтвердил обе границы переписки.');
      warnings.add('Ответы в неоткрытых тредах и содержимое файлов не входят в выгрузку.');
      return { detected: true, chatTitle, messages: result, coverage,
        metadata: { chatTitle, exportedAt: new Date().toISOString(), coverage, warnings: Array.from(warnings) } };
    } catch (error) {
      return { error: error.message, detected: true };
    } finally {
      root.__chatDownloaderRunning = false;
    }
  }

  root.ChatExtractor = { run };
})(globalThis);
