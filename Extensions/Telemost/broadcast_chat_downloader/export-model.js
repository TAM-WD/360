(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.ChatExport = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  const text = value => value == null ? '' : String(value);

  function normalizeMessage(source, position, usedIds) {
    let id = text(source.id);
    if (!id) {
      id = `export-message-${position + 1}`;
      while (usedIds.has(id)) id += '-';
    }
    const replyToId = text(source.replyToId);
    const questionText = text(source.questionText);
    const questionAuthor = text(source.questionAuthor).trim();
    const sender = text(source.sender).trim() || (source.isOwnMessage ? 'Вы' : 'Неизвестный автор');
    const type = source.type === 'Системное' ? 'Системное'
      : source.type === 'Ответ' || replyToId || questionText || questionAuthor ? 'Ответ' : 'Сообщение';
    return {
      id,
      time: text(source.time),
      date: text(source.date),
      sender,
      isOwnMessage: Boolean(source.isOwnMessage),
      message: text(source.message),
      replyToId,
      questionText,
      questionAuthor,
      attachments: Array.isArray(source.attachments) ? source.attachments.map(text).filter(Boolean) : [],
      type,
      order: Number.isFinite(source.order) ? source.order : position
    };
  }

  function content(message) {
    if (!message.attachments.length) return message.message;
    return `${message.message ? message.message + '\n\n' : ''}Вложения:\n${message.attachments.join('\n')}`;
  }

  function buildExportData(input, metadata = {}) {
    if (!Array.isArray(input)) throw new TypeError('Ожидался массив сообщений');
    const usedIds = new Set(input.map(message => text(message && message.id)).filter(Boolean));
    const byId = new Map();
    input.forEach((source, position) => {
      const message = normalizeMessage(source || {}, position, usedIds);
      usedIds.add(message.id);
      // DOM snapshots can include the same message repeatedly. Text never identifies a message.
      if (!byId.has(message.id)) byId.set(message.id, message);
    });
    const messages = Array.from(byId.values()).sort((a, b) => a.order - b.order);
    const quoteIndex = new Map();
    messages.forEach(message => {
      const key = JSON.stringify([message.message, message.sender]);
      const candidates = quoteIndex.get(key) || [];
      candidates.push(message);
      quoteIndex.set(key, candidates);
    });

    const groups = new Map();
    function ensureGroup(message) {
      if (!groups.has(message.id)) groups.set(message.id, { question: message, answers: [] });
      return groups.get(message.id);
    }
    messages.filter(message => message.type === 'Сообщение').forEach(ensureGroup);
    const orphans = [];
    messages.filter(message => message.type === 'Ответ').forEach(answer => {
      let parent = answer.replyToId ? byId.get(answer.replyToId) : null;
      let matchedBy = parent ? 'id' : 'unmatched';
      // An explicit target outside the collected history must stay unresolved.
      if (!answer.replyToId && answer.questionText && answer.questionAuthor) {
        const candidates = (quoteIndex.get(JSON.stringify([answer.questionText, answer.questionAuthor])) || [])
          .filter(candidate => candidate.order < answer.order);
        if (candidates.length === 1 && candidates[0].id !== answer.id) {
          parent = candidates[0];
          matchedBy = 'quote';
        }
      }
      if (parent && parent.id !== answer.id) ensureGroup(parent).answers.push({ answer, matchedBy });
      else orphans.push({ answer, matchedBy: 'unmatched' });
    });

    const entries = Array.from(groups.values()).map(group => ({ ...group, order: group.question.order }));
    orphans.forEach(({ answer, matchedBy }) => entries.push({
      question: null,
      answers: [{ answer, matchedBy }],
      order: answer.order
    }));
    entries.sort((a, b) => a.order - b.order);
    const pairs = [];
    entries.forEach((group, groupIndex) => {
      const answers = group.answers.length ? group.answers : [{ answer: null, matchedBy: '' }];
      answers.forEach(({ answer, matchedBy }, answerIndex) => {
        const showQuestion = answerIndex === 0;
        pairs.push({
          index: groupIndex + 1,
          questionTime: showQuestion && group.question ? group.question.time : '',
          questionAuthor: showQuestion ? (group.question ? group.question.sender : answer.questionAuthor) : '',
          questionText: showQuestion ? (group.question ? content(group.question) : answer.questionText) : '',
          answerTime: answer ? answer.time : '',
          answerAuthor: answer ? answer.sender : '',
          answerText: answer ? content(answer) : '',
          questionId: group.question ? group.question.id : '',
          answerId: answer ? answer.id : '',
          matchedBy,
          answersCount: group.answers.length,
          isFirstAnswer: Boolean(answer) && answerIndex === 0
        });
      });
    });

    return {
      messages,
      pairs,
      stats: {
        messages: messages.length,
        questions: messages.filter(message => message.type === 'Сообщение').length,
        answers: messages.filter(message => message.type === 'Ответ').length,
        attachments: messages.reduce((count, message) => count + message.attachments.length, 0)
      },
      metadata: { ...metadata, version: text(metadata.version) || '1.3' }
    };
  }

  function buildWorkbook(data, XLSX) {
    if (!XLSX || !XLSX.utils) throw new TypeError('Библиотека XLSX недоступна');
    const workbook = XLSX.utils.book_new();
    function append(name, rows, widths) {
      // aoa_to_sheet receives primitive strings, so '=' remains literal text, never a formula.
      const sheet = XLSX.utils.aoa_to_sheet(rows);
      sheet['!cols'] = widths.map(wch => ({ wch }));
      sheet['!autofilter'] = { ref: sheet['!ref'] };
      XLSX.utils.book_append_sheet(workbook, sheet, name);
      return sheet;
    }
    append('Сообщения', [
      ['ID сообщения', 'Дата', 'Время', 'Автор', 'Тип', 'Текст', 'Вложения', 'Ответ на ID', 'Цитата', 'Автор цитаты'],
      ...data.messages.map(message => [
        message.id, message.date, message.time, message.sender, message.type, message.message,
        message.attachments.join('\n'), message.replyToId, message.questionText, message.questionAuthor
      ])
    ], [30, 14, 16, 24, 14, 65, 55, 30, 55, 24]);
    const pairsSheet = append('Вопросы и ответы', [
      ['№', 'Время вопроса', 'Автор вопроса', 'Вопрос', 'Время ответа', 'Автор ответа', 'Ответ', 'ID вопроса', 'ID ответа', 'Связь'],
      ...data.pairs.map(pair => [
        pair.index, pair.questionTime, pair.questionAuthor, pair.questionText,
        pair.answerTime, pair.answerAuthor, pair.answerText, pair.questionId, pair.answerId,
        pair.matchedBy === 'id' ? 'По ID' : pair.matchedBy === 'quote' ? 'По уникальной цитате' : pair.matchedBy === 'unmatched' ? 'Оригинал не найден' : ''
      ])
    ], [6, 16, 24, 65, 16, 24, 65, 30, 30, 25]);
    pairsSheet['!merges'] = [];
    data.pairs.forEach((pair, index) => {
      if (pair.isFirstAnswer && pair.answersCount > 1) {
        for (let column = 0; column < 4; column++) {
          pairsSheet['!merges'].push({
            s: { r: index + 1, c: column }, e: { r: index + pair.answersCount, c: column }
          });
        }
      }
    });
    const metadata = data.metadata || {};
    const warnings = Array.isArray(metadata.warnings) ? metadata.warnings.map(text).join('\n') : text(metadata.warnings);
    const coverage = metadata.coverage && typeof metadata.coverage === 'object'
      ? JSON.stringify(metadata.coverage) : text(metadata.coverage);
    append('Об экспорте', [
      ['Параметр', 'Значение'],
      ['Название чата', text(metadata.chatTitle)],
      ['Время экспорта', text(metadata.exportedAt)],
      ['Версия расширения', text(metadata.version)],
      ['Охват истории', coverage],
      ['Предупреждения', warnings],
      ['Сообщений', data.stats.messages],
      ['Обычных сообщений', data.stats.questions],
      ['Ответов', data.stats.answers],
      ['Вложений', data.stats.attachments]
    ], [28, 100]);
    return workbook;
  }

  return { buildExportData, buildWorkbook };
});
