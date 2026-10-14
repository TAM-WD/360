(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  let current;
  const status = (message, error = false) => {
    $('status').textContent = message;
    $('status').className = `status${error ? ' error notice' : ''}`;
  };
  function render(snapshot) {
    current = snapshot;
    const policy = snapshot.error || (!snapshot.controls.canEdit ? 'Настройки заданы организацией. Пользовательские изменения запрещены.' : snapshot.managed ? 'Часть настроек может задаваться организацией.' : '');
    $('policy').textContent = policy;
    $('policy').hidden = !policy;
    const list = $('rules'); list.replaceChildren();
    for (const rule of snapshot.config.rules) {
      const row = document.createElement('div'); row.className = 'rule';
      const label = document.createElement('label');
      const toggle = document.createElement('input'); toggle.type = 'checkbox'; toggle.checked = rule.enabled;
      toggle.dataset.ruleId = rule.id; toggle.disabled = !snapshot.controls.rules[rule.id]?.canToggle;
      toggle.addEventListener('change', async () => {
        toggle.disabled = true;
        try { await change({ type: 'set-enabled', id: rule.id, enabled: toggle.checked }); status('Сохранено'); }
        catch (error) { status(error.message, true); await refresh(); }
      });
      const text = document.createElement('span'), name = document.createElement('strong');
      name.textContent = rule.name;
      text.append(name);
      if (toggle.disabled) {
        const lock = document.createElement('small'); lock.textContent = 'Задано конфигурацией'; text.append(lock);
      }
      label.append(toggle, text); row.append(label);
      if ($('custom-section') && snapshot.controls.rules[rule.id]?.canEdit) {
        for (const [title, event] of [['Изменить', 'edit-rule'], ['Удалить', 'remove-rule']]) {
          const button = document.createElement('button'); button.textContent = title;
          button.addEventListener('click', () => document.dispatchEvent(new CustomEvent(event, { detail: rule })));
          row.append(button);
        }
      }
      list.append(row);
    }
    if ($('editor-fields')) $('editor-fields').disabled = !snapshot.controls.canAdd;
    if ($('import-fields')) $('import-fields').disabled = !snapshot.controls.canEdit;
    if ($('reset-settings')) $('reset-settings').disabled = !snapshot.controls.canEdit;
    document.dispatchEvent(new CustomEvent('config-rendered', { detail: snapshot }));
  }
  async function refresh() {
    const result = await chrome.runtime.sendMessage({ type: 'get-config' });
    if (!result?.config) throw new Error(result?.error || 'Не удалось загрузить настройки.');
    render(result);
    return result;
  }
  async function change(command) {
    const result = await chrome.runtime.sendMessage({ type: 'change-config', command });
    if (result?.error) throw new Error(result.error);
    render(result);
    return result;
  }
  $('open-settings')?.addEventListener('click', () => chrome.runtime.openOptionsPage());
  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === 'managed' || (area === 'local' && changes.effectiveConfig)) void refresh().catch(error => status(error.message, true));
  });
  globalThis.SettingsUI = Object.freeze({ refresh, change, status, get current() { return current; } });
  void refresh().catch(error => status(error.message, true));
})();
