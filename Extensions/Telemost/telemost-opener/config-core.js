(() => {
  'use strict';
  const copy = value => JSON.parse(JSON.stringify(value));
  function settingsFile(input) {
    if (!input || input.schemaVersion !== 1 || typeof input.allowUserChanges !== 'boolean' || typeof input.allowCustomRules !== 'boolean') throw new Error('Некорректный файл настроек.');
    const result = { schemaVersion: 1, allowUserChanges: input.allowUserChanges, allowCustomRules: input.allowCustomRules };
    if (input.enabled !== undefined) {
      if (!input.enabled || typeof input.enabled !== 'object' || Array.isArray(input.enabled) || Object.entries(input.enabled).some(([id, value]) => !/^[a-zA-Z0-9_-]{1,80}$/.test(id) || ['__proto__', 'prototype', 'constructor'].includes(id) || typeof value !== 'boolean')) throw new Error('Некорректные переключатели в файле настроек.');
      result.enabled = copy(input.enabled);
    }
    return result;
  }
  function rulesFile(input, custom = false) {
    if (!input || input.schemaVersion !== 1 || !Array.isArray(input.rules)) throw new Error('Некорректный файл правил.');
    const validated = TelemostRouter.validateConfig({ schemaVersion: 1, allowUserChanges: true, allowCustomRules: true, rules: input.rules });
    if (custom && validated.rules.some(rule => !rule.id.startsWith('custom-'))) throw new Error('Пользовательские правила должны иметь id с префиксом custom-.');
    return { schemaVersion: 1, rules: validated.rules };
  }
  function combine(settings, rules) {
    const config = { ...settingsFile(settings), rules: rulesFile(rules).rules };
    for (const rule of config.rules) if (typeof config.enabled?.[rule.id] === 'boolean') rule.enabled = config.enabled[rule.id];
    delete config.enabled;
    return TelemostRouter.validateConfig(config);
  }
  function resolve(base, user = {}) {
    const config = TelemostRouter.validateConfig(base);
    const controls = { canEdit: base.allowUserChanges, canAdd: base.allowUserChanges && base.allowCustomRules, rules: {} };
    for (const rule of config.rules) {
      const canToggle = base.allowUserChanges && rule.userToggleAllowed;
      if (canToggle && typeof user.enabled?.[rule.id] === 'boolean') rule.enabled = user.enabled[rule.id];
      controls.rules[rule.id] = { canToggle, canEdit: false, source: 'config' };
    }
    if (controls.canAdd && Array.isArray(user.rules)) {
      for (const input of user.rules) {
        const rule = TelemostRouter.validateRule(input);
        if (!rule.id.startsWith('custom-') || config.rules.some(item => item.id === rule.id)) throw new Error('Некорректный id пользовательского правила.');
        // User rules cannot create their own administrative locks.
        rule.userToggleAllowed = true;
        config.rules.push(rule);
        controls.rules[rule.id] = { canToggle: true, canEdit: true, source: 'user' };
      }
    }
    if (config.rules.length > 100) throw new Error('Допускается не более 100 правил.');
    return { config, controls };
  }
  function change(base, stored, command) {
    if (command.type === 'import-file') {
      const input = command.document;
      if (input && Object.hasOwn(input, 'allowUserChanges')) {
        if (Object.hasOwn(input, 'rules')) return change(base, stored, { type: 'import-config', config: input });
        return change(base, stored, { type: 'import-settings', settings: settingsFile(input) });
      }
      return change(base, stored, { type: 'import-user-rules', document: rulesFile(input, true) });
    }
    const user = { enabled: { ...(stored.enabled || {}) }, rules: copy(stored.rules || []) };
    const snapshot = resolve(base, user);
    if (!snapshot.controls.canEdit) throw new Error('Изменения запрещены конфигурацией организации.');
    if (command.type === 'set-enabled') {
      const control = snapshot.controls.rules[command.id];
      if (!control?.canToggle || typeof command.enabled !== 'boolean') throw new Error('Этот переключатель заблокирован.');
      if (control.source === 'user') user.rules.find(rule => rule.id === command.id).enabled = command.enabled;
      else user.enabled[command.id] = command.enabled;
    } else if (command.type === 'save-custom') {
      if (!snapshot.controls.canAdd) throw new Error('Собственные правила запрещены конфигурацией.');
      const rule = TelemostRouter.anonymizeExamples(command.rule);
      if (!rule.id.startsWith('custom-') || base.rules.some(item => item.id === rule.id)) throw new Error('Некорректный id пользовательского правила.');
      rule.userToggleAllowed = true;
      const index = user.rules.findIndex(item => item.id === rule.id);
      if (index < 0) user.rules.push(rule); else user.rules[index] = rule;
    } else if (command.type === 'remove-custom') {
      if (!snapshot.controls.rules[command.id]?.canEdit) throw new Error('Это правило нельзя удалить.');
      user.rules = user.rules.filter(rule => rule.id !== command.id);
    } else if (command.type === 'import-config') {
      const incoming = TelemostRouter.validateConfig(command.config);
      // Import only user-editable values. Never copy incoming policy flags,
      // templates of installed rules, or their userToggleAllowed properties.
      for (const rule of incoming.rules) {
        const control = snapshot.controls.rules[rule.id];
        if (control?.source === 'config' && control.canToggle) user.enabled[rule.id] = rule.enabled;
      }
      if (snapshot.controls.canAdd) {
        const custom = incoming.rules.filter(rule => !base.rules.some(item => item.id === rule.id)).map(TelemostRouter.anonymizeExamples);
        for (const rule of custom) {
          if (!rule.id.startsWith('custom-')) throw new Error('Id импортируемого собственного правила должен начинаться с custom-.');
          rule.userToggleAllowed = true;
        }
        user.rules = custom;
      }
    } else if (command.type === 'import-settings') {
      const incoming = settingsFile(command.settings);
      for (const [id, enabled] of Object.entries(incoming.enabled || {})) {
        const control = snapshot.controls.rules[id];
        if (control?.source === 'config' && control.canToggle) user.enabled[id] = enabled;
      }
    } else if (command.type === 'import-user-rules') {
      if (!snapshot.controls.canAdd) throw new Error('Собственные правила запрещены конфигурацией.');
      user.rules = rulesFile(command.document, true).rules.map(input => {
        if (base.rules.some(rule => rule.id === input.id)) throw new Error('Id пользовательского правила занят правилом поставки.');
        return { ...TelemostRouter.anonymizeExamples(input), userToggleAllowed: true };
      });
    } else if (command.type === 'reset-settings') {
      return { enabled: {}, rules: [] };
    } else throw new Error('Неизвестная операция.');
    resolve(base, user);
    return user;
  }
  globalThis.ConfigCore = Object.freeze({ resolve, change, settingsFile, rulesFile, combine });
})();
