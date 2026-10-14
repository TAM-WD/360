(() => {
  'use strict';
  let snapshot = { config: { schemaVersion: 1, allowUserChanges: false, allowCustomRules: false, rules: [] }, controls: { canEdit: false, canAdd: false, rules: {} }, error: null };
  let base;
  let revision = 0;
  async function bundledFile(name) {
    const response = await fetch(chrome.runtime.getURL(name));
    if (!response.ok) throw new Error(`Не удалось прочитать ${name}.`);
    return response.json();
  }
  const bundled = Promise.all([bundledFile('config.json'), bundledFile('rules.json')]).then(([settings, rules]) => ({ settings, rules }));
  const managedRead = () => chrome.storage.managed.get(null).catch(() => ({}));
  async function loadUser() {
    const local = await chrome.storage.local.get('userSettings');
    const settings = local.userSettings || {};
    let document = await UserRulesFile.read();
    if (!document && settings.rulesFileInitialized) throw new Error('Файл user-rules.json отсутствует. Восстановите его из резервной копии профиля.');
    const legacy = Array.isArray(settings.rules) ? settings.rules : [];
    if (!document || (legacy.length && !settings.rulesFileInitialized)) {
      const rules = document?.rules || [];
      const ids = new Set(rules.map(rule => rule.id));
      document = await UserRulesFile.write({ schemaVersion: 1, rules: [...rules, ...legacy.filter(rule => !ids.has(rule.id))] });
    }
    if (!settings.rulesFileInitialized || Object.hasOwn(settings, 'rules')) {
      // Remove old copies only after the file has been validated and committed.
      await chrome.storage.local.set({ userSettings: { enabled: settings.enabled || {}, rulesFileInitialized: true } });
    }
    return { enabled: settings.enabled || {}, rules: document.rules };
  }
  async function reload() {
    base = null;
    try {
      const [defaults, managed] = await Promise.all([bundled, managedRead()]);
      const settings = managed.ConfigJSON === undefined ? defaults.settings : JSON.parse(managed.ConfigJSON);
      const rules = managed.RulesJSON !== undefined ? JSON.parse(managed.RulesJSON)
        : Array.isArray(settings.rules) ? { schemaVersion: 1, rules: settings.rules } : defaults.rules;
      base = ConfigCore.combine(settings, rules);
      if (typeof managed.AllowUserChanges === 'boolean') base.allowUserChanges = managed.AllowUserChanges;
      if (typeof managed.AllowCustomRules === 'boolean') base.allowCustomRules = managed.AllowCustomRules;
      const user = await loadUser();
      snapshot = { ...ConfigCore.resolve(base, user), error: null, managed: Object.keys(managed).length > 0, revision: ++revision };
    } catch (error) {
      snapshot = { config: { schemaVersion: 1, allowUserChanges: false, allowCustomRules: false, rules: [] }, controls: { canEdit: !!base?.allowUserChanges, canAdd: false, rules: {} }, error: `Настройки не загружены: ${error.message}`, revision: ++revision };
    }
    // This is a derived cache for content scripts, not the source of user rules.
    await chrome.storage.local.set({ effectiveConfig: snapshot.config });
    return snapshot;
  }
  let queue = reload();
  function schedule(operation) {
    const result = queue.then(operation, operation);
    queue = result.catch(() => {});
    return result;
  }
  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === 'managed' || (area === 'local' && changes.userSettings)) void schedule(reload);
  });
  async function execute(command) {
    return schedule(async () => {
      await reload();
      if (!base) throw new Error(snapshot.error);
      let current, recovering = false;
      try { current = await loadUser(); }
      catch (error) {
        const replacement = command.type === 'reset-settings' || command.type === 'import-user-rules' || command.type === 'import-config' || (command.type === 'import-file' && Array.isArray(command.document?.rules));
        if (!replacement) throw error;
        const local = await chrome.storage.local.get('userSettings');
        current = { enabled: local.userSettings?.enabled || {}, rules: [] };
        recovering = true;
      }
      const user = ConfigCore.change(base, current, command);
      // Never report success or update the cache before the file is committed.
      if (recovering || JSON.stringify(user.rules) !== JSON.stringify(current.rules)) {
        await UserRulesFile.write({ schemaVersion: 1, rules: user.rules });
      }
      await chrome.storage.local.set({ userSettings: { enabled: user.enabled, rulesFileInitialized: true } });
      return reload();
    });
  }
  async function exportFiles() {
    return schedule(async () => {
      await reload();
      if (!base) throw new Error(snapshot.error);
      const user = await loadUser();
      return {
        settings: { schemaVersion: 1, allowUserChanges: base.allowUserChanges, allowCustomRules: base.allowCustomRules,
          enabled: Object.fromEntries(snapshot.config.rules.filter(rule => snapshot.controls.rules[rule.id].source === 'config').map(rule => [rule.id, rule.enabled])) },
        rules: { schemaVersion: 1, rules: base.rules },
        userRules: { schemaVersion: 1, rules: user.rules }
      };
    });
  }
  globalThis.ConfigService = Object.freeze({
    get ready() { return queue; },
    get snapshot() { return snapshot; },
    toAppUrl: (value, urlBase) => TelemostRouter.toAppUrl(value, urlBase, snapshot.config),
    execute, exportFiles
  });
})();
