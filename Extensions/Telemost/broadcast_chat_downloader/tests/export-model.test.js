'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const XLSX = require('../libs/xlsx.full.min.js');
const { buildExportData, buildWorkbook } = require('../export-model.js');

const message = (id, message, extra = {}) => ({ id, message, sender: 'Анна', ...extra });

test('deduplicates IDs while preserving repeated text, order and the input', () => {
  const input = [message('2', 'Повтор', { order: 2 }), message('1', 'Повтор', { order: 1 }), message('1', 'Повтор')];
  const original = JSON.stringify(input);
  const data = buildExportData(input);
  assert.deepEqual(data.messages.map(item => item.id), ['1', '2']);
  assert.equal(data.pairs.length, 2);
  assert.deepEqual(data.stats, { messages: 2, questions: 2, answers: 0, attachments: 0 });
  assert.equal(JSON.stringify(input), original);
});

test('uses explicit IDs ahead of quote text and keeps every nested and orphan answer', () => {
  const data = buildExportData([
    message('q1', 'Вопрос', { time: '10:00' }),
    message('q2', 'Вопрос', { time: '10:01' }),
    message('a1', 'Первый ответ', { type: 'Ответ', replyToId: 'q2', questionText: 'Несовпадение', questionAuthor: 'Анна' }),
    message('a2', 'Второй ответ', { type: 'Ответ', replyToId: 'q2' }),
    message('a3', 'Вложенный ответ', { type: 'Ответ', replyToId: 'a1' }),
    message('a4', 'Ответ на недоступное сообщение', { type: 'Ответ', replyToId: 'missing', questionText: 'Вопрос', questionAuthor: 'Анна' }),
    message('a5', 'Неоднозначная цитата', { type: 'Ответ', questionText: 'Вопрос', questionAuthor: 'Анна' })
  ]);
  const answers = data.pairs.filter(pair => pair.answerId);
  assert.deepEqual(answers.map(pair => pair.answerId).sort(), ['a1', 'a2', 'a3', 'a4', 'a5']);
  assert.equal(answers.find(pair => pair.answerId === 'a1').questionId, 'q2');
  assert.equal(answers.find(pair => pair.answerId === 'a3').questionId, 'a1');
  assert.equal(answers.find(pair => pair.answerId === 'a4').questionId, '');
  assert.equal(answers.find(pair => pair.answerId === 'a5').questionId, '');
  assert.deepEqual(data.stats, { messages: 7, questions: 2, answers: 5, attachments: 0 });
  assert.equal(data.pairs.find(pair => pair.answerId === 'a1').answersCount, 2);
});

test('quote fallback requires a unique exact text and author, without case normalization', () => {
  const data = buildExportData([
    message('q1', 'Да'), message('q2', 'Да', { sender: 'Борис' }),
    message('a1', 'Ответ', { type: 'Ответ', questionText: 'Да', questionAuthor: 'Анна' }),
    message('a2', 'Другой ответ', { type: 'Ответ', questionText: 'да', questionAuthor: 'Анна' }),
    message('a3', 'Без автора', { type: 'Ответ', questionText: 'Да' })
  ]);
  assert.equal(data.pairs.find(pair => pair.answerId === 'a1').questionId, 'q1');
  assert.equal(data.pairs.find(pair => pair.answerId === 'a1').matchedBy, 'quote');
  assert.equal(data.pairs.find(pair => pair.answerId === 'a2').questionId, '');
  assert.equal(data.pairs.find(pair => pair.answerId === 'a3').questionId, '');
});

test('keeps own/unknown authors, system messages and attachment-only messages', () => {
  const data = buildExportData([
    message('own', '', { sender: null, isOwnMessage: true, attachments: ['https://example.test/file.pdf'] }),
    message('incoming', 'Привет', { sender: '' }),
    message('named-own', 'Привет', { isOwnMessage: true, sender: 'Александр' }),
    message('system', 'Пользователь присоединился', { type: 'Системное', sender: '' }),
    message('answer', '', { type: 'Ответ', sender: null, replyToId: 'own', attachments: ['Фото', 'Архив'] })
  ]);
  assert.deepEqual(data.messages.map(item => item.sender), ['Вы', 'Неизвестный автор', 'Александр', 'Неизвестный автор', 'Неизвестный автор']);
  assert.deepEqual(data.stats, { messages: 5, questions: 3, answers: 1, attachments: 3 });
  assert.match(data.pairs.find(pair => pair.answerId === 'answer').answerText, /Фото\nАрхив/);
  assert.match(data.pairs.find(pair => pair.questionId === 'own').questionText, /file\.pdf/);
  assert.ok(data.pairs.every(pair => pair.questionId !== 'system'));
});

test('missing IDs stay separate and do not collide with real IDs', () => {
  const data = buildExportData([message('export-message-1', 'Реальный ID'), message('', 'Текст'), message(null, 'Текст')]);
  assert.equal(data.messages.length, 3);
  assert.equal(new Set(data.messages.map(item => item.id)).size, 3);
});

test('real XLSX round trip retains all content and literal cells, including formula-like text', () => {
  const rawText = '=HYPERLINK("https://example.test","текст")\nВторая строка';
  const data = buildExportData([
    message('001', rawText, { date: '05.10.2026', time: '09:12', attachments: ['=опасное имя.xlsx'] }),
    message('002', '+SUM(1,2)', { type: 'Ответ', replyToId: '001', questionText: rawText, questionAuthor: 'Анна' }),
    message('003', '@Ответ', { type: 'Ответ', replyToId: '001' }),
    message('004', 'Системное событие', { type: 'Системное' })
  ], {
    chatTitle: '=Чат', exportedAt: '2026-10-05T09:15:00+03:00', version: '1.3',
    coverage: 'Доступная история', warnings: ['История может быть ограничена правами', 'Стабильная граница прокрутки']
  });
  const workbook = buildWorkbook(data, XLSX);
  const bytes = Buffer.from(XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' }));
  assert.equal(bytes.subarray(0, 2).toString(), 'PK');
  const restored = XLSX.read(bytes, { type: 'buffer' });
  assert.deepEqual(restored.SheetNames, ['Сообщения', 'Вопросы и ответы', 'Об экспорте']);
  const rows = XLSX.utils.sheet_to_json(restored.Sheets['Сообщения'], { header: 1, defval: '' });
  assert.equal(rows.length, 5);
  assert.deepEqual(rows[1], ['001', '05.10.2026', '09:12', 'Анна', 'Сообщение', rawText, '=опасное имя.xlsx', '', '', '']);
  assert.equal(rows[2][7], '001');
  assert.equal(rows[4][4], 'Системное');
  assert.equal(restored.Sheets['Сообщения'].F2.t, 's');
  assert.equal(restored.Sheets['Сообщения'].F2.f, undefined);
  assert.equal(restored.Sheets['Сообщения'].G2.f, undefined);
  const pairs = XLSX.utils.sheet_to_json(restored.Sheets['Вопросы и ответы'], { header: 1, defval: '' });
  assert.deepEqual(pairs[0].slice(0, 7), ['№', 'Время вопроса', 'Автор вопроса', 'Вопрос', 'Время ответа', 'Автор ответа', 'Ответ']);
  assert.deepEqual(pairs[0].slice(7), ['ID вопроса', 'ID ответа', 'Связь']);
  assert.equal(pairs[1][9], 'По ID');
  assert.deepEqual(pairs.slice(1).map(row => row[6]), ['+SUM(1,2)', '@Ответ']);
  assert.equal(restored.Sheets['Вопросы и ответы']['!merges'].length, 4);
  const metadata = XLSX.utils.sheet_to_json(restored.Sheets['Об экспорте'], { header: 1, defval: '' });
  assert.ok(metadata.some(row => row[0] === 'Версия расширения' && row[1] === '1.3'));
  assert.ok(metadata.some(row => row[0] === 'Предупреждения' && row[1].includes('\n')));
  for (const name of restored.SheetNames) {
    for (const [address, cell] of Object.entries(restored.Sheets[name])) {
      if (!address.startsWith('!')) assert.equal(cell.f, undefined);
    }
  }
});

test('quote matching cannot link an answer to a later message', () => {
  const data = buildExportData([
    { id: 'reply', order: 0, sender: 'Борис', type: 'Ответ', message: 'Ответ', questionText: 'Привет', questionAuthor: 'Анна' },
    { id: 'later', order: 1, sender: 'Анна', message: 'Привет' }
  ]);
  const reply = data.pairs.find(pair => pair.answerId === 'reply');
  assert.equal(reply.questionId, '');
  assert.equal(reply.matchedBy, 'unmatched');
});

test('loads as a browser global without CommonJS', () => {
  const context = vm.createContext({});
  vm.runInContext(fs.readFileSync(require.resolve('../export-model.js'), 'utf8'), context);
  assert.equal(typeof context.ChatExport.buildExportData, 'function');
  assert.equal(context.ChatExport.buildExportData([]).stats.messages, 0);
});
