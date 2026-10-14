(() => {
  "use strict";
  const deniedSchemes = new Set(['http', 'https', 'file', 'ftp', 'ftps', 'javascript', 'vbscript', 'data', 'blob', 'about', 'chrome', 'chrome-extension', 'edge', 'browser', 'view-source', 'shell', 'powershell', 'cmd', 'applescript']);
  const tokenPattern = /\{(url|urlNoScheme|origin|host|pathname|pathAndQuery|search|hash|fragment|tail|hashPathname|hashPathAndQuery|hashTail|hashPath:\d+|path:\d+|capture:[a-zA-Z][a-zA-Z0-9_]{0,39}|hashCapture:[a-zA-Z][a-zA-Z0-9_]{0,39}|query:[^{}|]+)(\|encode)?\}/g;
  const clone = value => JSON.parse(JSON.stringify(value));
  const fail = message => { throw new Error(message); };
  const isObject = value => value && typeof value === 'object' && !Array.isArray(value);
  function text(value, name, max = 8192) {
    if (typeof value !== 'string' || !value || value.length > max || /[\x00-\x1f\x7f]/.test(value)) fail(`Некорректное поле «${name}».`);
    return value;
  }
  function webUrl(value, base) {
    try {
      const url = new URL(value, base);
      return ['https:', 'http:'].includes(url.protocol) && !url.username && !url.password ? url : null;
    } catch { return null; }
  }
  function external(value) {
    if (typeof value !== 'string' || /[\x00-\x20\x7f]/.test(value)) return false;
    const scheme = /^([a-z][a-z0-9+.-]+):/i.exec(value)?.[1].toLowerCase();
    return !!scheme && !deniedSchemes.has(scheme);
  }
  function rawQuery(url, key) {
    const values = [];
    for (const pair of url.search.slice(1).split('&')) {
      const index = pair.indexOf('=');
      const name = index < 0 ? pair : pair.slice(0, index);
      try {
        if (decodeURIComponent(name.replace(/\+/g, ' ')) === key) values.push(index < 0 ? '' : pair.slice(index + 1));
      } catch { /* An unrelated malformed parameter does not affect this rule. */ }
    }
    return values.length === 1 ? values[0] : null;
  }
  function hashRoute(url) {
    const fragment = url.hash.slice(1);
    if (!fragment.startsWith('/') || fragment.includes('#')) return null;
    const queryAt = fragment.indexOf('?');
    return { pathname: queryAt < 0 ? fragment : fragment.slice(0, queryAt), pathAndQuery: fragment };
  }
  // One capture per segment, with optional literal prefix/suffix. No user regex.
  function segmentVariable(part) {
    const variable = /^([^{}]*)\{([a-zA-Z][a-zA-Z0-9_]{0,39})\}([^{}]*)$/.exec(part);
    return variable?.[2] === 'tail' ? null : variable;
  }
  function pathMatch(pattern, pathname) {
    const expected = pattern.split('/');
    const actual = pathname.split('/');
    const captures = Object.create(null);
    for (let i = 0; i < expected.length; i++) {
      if (expected[i] === '{tail}') {
        if (!actual[i]) return null;
        return { tail: actual.slice(i).join('/'), captures };
      }
      const variable = segmentVariable(expected[i]);
      if (variable) {
        const [, prefix, name, suffix] = variable;
        const value = actual[i];
        if (!value || !value.startsWith(prefix) || !value.endsWith(suffix) || value.length <= prefix.length + suffix.length) return null;
        const capture = value.slice(prefix.length, value.length - suffix.length);
        // Duplicate old {pN} labels remain valid for positional {path:N}, but
        // cannot be referenced by name because the capture would be ambiguous.
        captures[name] = Object.hasOwn(captures, name) ? null : capture;
      } else if (expected[i] !== actual[i]) return null;
    }
    return expected.length === actual.length ? { tail: '', captures } : null;
  }
  function matches(rule, url) {
    const m = rule.match;
    if (!m.origins.includes(url.origin)) return null;
    if (m.exactUrl && url.href !== m.exactUrl) return null;
    const match = pathMatch(m.pathname, url.pathname);
    if (!match || (m.hashRequired && url.hash.length < 2)) return null;
    if (m.hashPathname !== undefined) {
      const route = hashRoute(url);
      const hashMatch = route && pathMatch(m.hashPathname, route.pathname);
      if (!hashMatch) return null;
      match.hashTail = hashMatch.tail;
      match.hashCaptures = hashMatch.captures;
    }
    if ((m.queryKeys || []).some(key => rawQuery(url, key) === null || rawQuery(url, key) === '')) return null;
    return match;
  }
  function render(template, url, match) {
    let valid = true;
    const route = hashRoute(url);
    const parts = url.pathname.split('/').slice(1);
    const hashParts = route?.pathname.split('/').slice(1);
    const values = {
      url: url.href, urlNoScheme: url.href.slice(url.protocol.length + 2),
      origin: url.origin, host: url.host, pathname: url.pathname,
      pathAndQuery: url.pathname + url.search + url.hash,
      search: url.search, hash: url.hash, fragment: url.hash.slice(1), tail: match.tail,
      hashPathname: route?.pathname, hashPathAndQuery: route?.pathAndQuery, hashTail: match.hashTail
    };
    const result = template.replace(tokenPattern, (_all, name, encode) => {
      let value = name.startsWith('path:') ? parts[Number(name.slice(5))]
        : name.startsWith('hashPath:') ? hashParts?.[Number(name.slice(9))]
        : name.startsWith('capture:') ? match.captures?.[name.slice(8)]
        : name.startsWith('hashCapture:') ? match.hashCaptures?.[name.slice(12)]
        : name.startsWith('query:') ? rawQuery(url, name.slice(6)) : values[name];
      if (value === null || value === undefined) { valid = false; return ''; }
      return encode ? encodeURIComponent(value) : value;
    });
    return valid && external(result) ? result : null;
  }
  function validateRule(input) {
    if (!isObject(input)) fail('Правило должно быть JSON-объектом.');
    const rule = clone(input);
    if (!/^[a-zA-Z0-9_-]{1,80}$/.test(text(rule.id, 'id', 80))) fail('Некорректный id правила.');
    if (['__proto__', 'prototype', 'constructor'].includes(rule.id)) fail('Зарезервированный id правила.');
    text(rule.name, 'название', 120);
    if (typeof rule.enabled !== 'boolean' || typeof rule.userToggleAllowed !== 'boolean') fail('enabled и userToggleAllowed должны быть true или false.');
    if (!isObject(rule.match) || !Array.isArray(rule.match.origins) || !rule.match.origins.length || rule.match.origins.length > 20) fail('Укажите от 1 до 20 точных origin.');
    for (const origin of rule.match.origins) {
      const url = webUrl(origin);
      if (!url || origin !== url.origin || /[\*{}]/.test(origin)) fail('Origin должен содержать только http(s), домен и необязательный порт, без пути и масок.');
    }
    for (const field of ['pathname', 'hashPathname']) {
      if (field === 'hashPathname' && rule.match[field] === undefined) continue;
      const pathname = text(rule.match[field], field);
      if (!pathname.startsWith('/') || /[?#]/.test(pathname)) fail(`${field} должен начинаться с / и не содержать query/hash.`);
      const segments = pathname.split('/');
      segments.forEach((part, index) => {
        if (!/[{}]/.test(part)) return;
        if (segmentVariable(part)) return;
        if (part === '{tail}' && index === segments.length - 1) return;
        fail('В одном сегменте допустима одна переменная с буквальным префиксом/суффиксом, например +{invite} или file-{id}.json. Имя: латинская буква, затем буквы, цифры или _. {tail} занимает весь последний сегмент.');
      });
    }
    if (rule.match.hashRequired !== undefined && typeof rule.match.hashRequired !== 'boolean') fail('hashRequired должен быть true или false.');
    if (rule.match.queryKeys !== undefined && (!Array.isArray(rule.match.queryKeys) || rule.match.queryKeys.length > 30 || rule.match.queryKeys.some(key => typeof key !== 'string' || !key || /[{}|\x00-\x1f]/.test(key)))) fail('Некорректный список queryKeys.');
    if (rule.match.exactUrl !== undefined) {
      const exact = webUrl(rule.match.exactUrl);
      if (!exact || exact.href !== rule.match.exactUrl || !matches(rule, exact)) fail('exactUrl не соответствует условиям правила.');
    }
    text(rule.target, 'шаблон приложения');
    if (!external(rule.target) || !/^[a-z][a-z0-9+.-]+:[^{}]*/i.test(rule.target)) fail('Нужна схема внешнего приложения, например telemost:// или zoommtg://.');
    if (/[{}]/.test(rule.target.replace(tokenPattern, ''))) fail('Неизвестная переменная в шаблоне.');
    if (!Array.isArray(rule.examples) || !rule.examples.length || rule.examples.length > 10) fail('Нужна хотя бы одна исходная пара ссылки и ассоциации.');
    for (const pair of rule.examples) {
      if (!isObject(pair)) fail('Некорректный пример.');
      const url = webUrl(text(pair.url, 'ссылка примера'));
      text(pair.appUrl, 'ассоциация примера');
      const match = url && matches(rule, url);
      if (!match || render(rule.target, url, match) !== pair.appUrl) fail('Шаблон не воспроизводит исходную пару ссылки и ассоциации.');
    }
    return rule;
  }
  function validateConfig(input) {
    if (!isObject(input) || input.schemaVersion !== 1 || typeof input.allowUserChanges !== 'boolean' || typeof input.allowCustomRules !== 'boolean' || !Array.isArray(input.rules) || input.rules.length > 100) fail('Некорректная структура JSON-конфига (schemaVersion: 1).');
    const result = clone(input);
    result.rules = input.rules.map(validateRule);
    if (new Set(result.rules.map(rule => rule.id)).size !== result.rules.length) fail('Id правил должны быть уникальны.');
    return result;
  }
  function anonymizeExamples(input) {
    const rule = validateRule(input);
    if (rule.match.exactUrl) fail('Нельзя сохранить тестовый пример для точного URL. Задайте переменные в условии и шаблоне, затем удалите match.exactUrl.');
    const replacements = new Map();
    let numeric = 123456, mixed = 3, hasSample = false;
    const sample = value => {
      if (!value) return value;
      hasSample = true;
      if (!replacements.has(value)) {
        replacements.set(value, /^-?\d+$/.test(value) ? `${value.startsWith('-') ? '-' : ''}${numeric++}` : `a1b2c${mixed++}`);
      }
      return replacements.get(value);
    };
    function samplePath(pattern, pathname) {
      const parts = pathname.split('/');
      for (const [index, part] of pattern.split('/').entries()) {
        if (part === '{tail}') {
          parts.splice(index, parts.length - index, ...parts.slice(index).map(sample));
          break;
        }
        const variable = segmentVariable(part);
        if (variable) {
          const [, prefix, , suffix] = variable;
          parts[index] = prefix + sample(parts[index].slice(prefix.length, parts[index].length - suffix.length)) + suffix;
        }
      }
      return parts.join('/');
    }
    function sampleQuery(search) {
      if (!search) return '';
      return '?' + search.slice(1).split('&').map(pair => {
        const index = pair.indexOf('=');
        return index < 0 ? pair : pair.slice(0, index + 1) + sample(pair.slice(index + 1));
      }).join('&');
    }
    rule.examples = rule.examples.map(pair => {
      hasSample = false;
      const url = webUrl(pair.url);
      url.pathname = samplePath(rule.match.pathname, url.pathname);
      url.search = sampleQuery(url.search);
      const route = hashRoute(url);
      if (route) {
        const pathname = rule.match.hashPathname === undefined
          ? route.pathname.split('/').map(sample).join('/')
          : samplePath(rule.match.hashPathname, route.pathname);
        url.hash = '#' + pathname + sampleQuery(route.pathAndQuery.slice(route.pathname.length));
      } else if (url.hash) url.hash = '#' + sample(url.hash.slice(1));
      const match = matches(rule, url);
      const appUrl = match && render(rule.target, url, match);
      if (!appUrl || !hasSample) fail('Для тестового примера нужны переменные части ссылки. Обобщите правило перед сохранением.');
      return { url: url.href, appUrl };
    });
    return validateRule(rule);
  }
  function toAppUrl(value, base, config) {
    if (!config) return null;
    const url = webUrl(value, base);
    if (!url) return null;
    for (const rule of config.rules) {
      const match = matches(rule, url);
      // First matching rule owns its scope even when switched off. A custom rule
      // cannot silently re-enable a disabled corporate/built-in route.
      if (match) return rule.enabled ? render(rule.target, url, match) : null;
    }
    return null;
  }
  globalThis.TelemostRouter = Object.freeze({ toAppUrl, validateConfig, validateRule, anonymizeExamples, webUrl, external, rawQuery, hashRoute, render, matches });
})();
