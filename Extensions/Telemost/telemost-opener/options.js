(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  let editingId;
  const report = action => async () => { try { await action(); } catch (error) { SettingsUI.status(error.message, true); } };
  function readRule() {
    const input = JSON.parse($('rule-json').value);
    if ($('rule-name').value.trim()) input.name = $('rule-name').value.trim();
    return TelemostRouter.validateRule(input);
  }
  function preview() {
    try {
      const rule = readRule();
      $('scope').textContent = `Область: ${rule.match.origins.join(', ')}${rule.match.pathname}${rule.match.hashPathname ? '#' + rule.match.hashPathname : ''}${rule.match.exactUrl ? ' · только исходный URL' : ''}`;
      const result = TelemostRouter.toAppUrl($('try-url').value.trim(), undefined, { rules: [{ ...rule, enabled: true }] });
      $('preview').textContent = result || 'Эта ссылка не соответствует правилу.';
      TelemostRouter.anonymizeExamples(rule); // Check that saving can omit real example values.
      $('save-rule').disabled = false;
    } catch (error) { $('preview').textContent = error.message; $('save-rule').disabled = true; }
  }
  function showRule(rule, note) {
    editingId = rule.id;
    $('rule-name').value = rule.name;
    $('source-url').value = rule.examples[0].url;
    $('app-url').value = rule.examples[0].appUrl;
    $('rule-json').value = JSON.stringify(rule, null, 2);
    $('try-url').value = rule.examples[0].url;
    $('inference-note').textContent = note;
    $('proposal').hidden = false;
    preview();
  }
  function clear() {
    editingId = undefined;
    for (const id of ['rule-name', 'source-url', 'app-url', 'rule-json', 'try-url']) $(id).value = '';
    $('proposal').hidden = true;
  }
  $('analyze').addEventListener('click', report(() => {
    const result = RuleInference.infer($('source-url').value, $('app-url').value, editingId);
    const saved = SettingsUI.current.config.rules.find(rule => rule.id === editingId);
    if (saved) result.rule.enabled = saved.enabled;
    if ($('rule-name').value.trim()) result.rule.name = $('rule-name').value.trim();
    showRule(result.rule, result.note);
    SettingsUI.status('Шаблон подготовлен. Проверьте подстановку и сохраните правило.');
  }));
  for (const id of ['try-url', 'rule-json', 'rule-name']) $(id).addEventListener('input', preview);
  for (const id of ['source-url', 'app-url']) $(id).addEventListener('input', () => {
    $('proposal').hidden = true; // Changed examples must be analyzed again before saving.
  });
  $('clear-editor').addEventListener('click', clear);
  $('save-rule').addEventListener('click', report(async () => {
    const rule = readRule();
    const earlier = SettingsUI.current.config.rules.find(item => item.id !== rule.id && rule.examples.some(pair => TelemostRouter.matches(item, new URL(pair.url))));
    if (earlier) throw new Error(`Этот пример уже охватывает правило «${earlier.name}». Измените область нового правила, чтобы оно могло сработать.`);
    await SettingsUI.change({ type: 'save-custom', rule });
    clear(); SettingsUI.status('Правило сохранено. Оно уже действует в открытых страницах.');
  }));
  document.addEventListener('edit-rule', event => {
    showRule(event.detail, 'Измените примеры и определите подстановку заново либо отредактируйте JSON.');
    $('custom-section').scrollIntoView({ behavior: 'smooth' });
  });
  document.addEventListener('remove-rule', event => { void report(async () => {
    await SettingsUI.change({ type: 'remove-custom', id: event.detail.id });
    if (editingId === event.detail.id) clear();
    SettingsUI.status('Правило удалено.');
  })(); });
  for (const [id, key, filename] of [['export-config', 'settings', 'config.json'], ['export-rules', 'rules', 'rules.json'], ['export-user-rules', 'userRules', 'user-rules.json']]) {
    $(id).addEventListener('click', report(async () => {
      const files = await chrome.runtime.sendMessage({ type: 'export-files' });
      if (files.error) throw new Error(files.error);
      const blob = new Blob([JSON.stringify(files[key], null, 2) + '\n'], { type: 'application/json' });
      const url = URL.createObjectURL(blob), link = document.createElement('a');
      link.href = url; link.download = filename; link.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      SettingsUI.status(`Файл ${filename} экспортирован.`);
    }));
  }
  $('import-file').addEventListener('change', report(async () => {
    const file = $('import-file').files[0];
    if (!file) return;
    if (file.size > 1024 * 1024) throw new Error('Размер конфига не должен превышать 1 МБ.');
    $('import-json').value = await file.text();
    SettingsUI.status('Файл прочитан. Нажмите «Применить конфиг» для импорта.');
  }));
  $('import-config').addEventListener('click', report(async () => {
    const document = JSON.parse($('import-json').value);
    await SettingsUI.change({ type: 'import-file', document });
    $('import-json').value = ''; $('import-file').value = '';
    clear(); SettingsUI.status('Конфиг применён. Ограничения организации сохранены.');
  }));
  $('reset-settings').addEventListener('click', report(async () => {
    await SettingsUI.change({ type: 'reset-settings' }); clear(); SettingsUI.status('Восстановлена конфигурация из поставки. Собственные правила удалены.');
  }));
})();
