(() => {
  'use strict';
  const R = TelemostRouter;
  function infer(urlText, appText, id = `custom-${crypto.randomUUID()}`) {
    const url = R.webUrl(urlText.trim());
    const appUrl = appText.trim();
    if (!url || !R.external(appUrl)) throw new Error('Укажите HTTP(S)-ссылку и адрес внешнего приложения.');
    const parts = url.pathname.split('/').slice(1);
    const hashRoute = R.hashRoute(url);
    const hashParts = hashRoute?.pathname.split('/').slice(1);
    const sectionPattern = (pathname, segments) => segments.length >= 2 && segments[0] && segments[1] ? `/${segments[0]}/{tail}` : pathname;
    const rule = {
      id, name: url.hostname, enabled: true, userToggleAllowed: true,
      match: { origins: [url.origin], pathname: url.pathname, hashRequired: false, queryKeys: [] },
      target: appUrl, examples: [{ url: url.href, appUrl }]
    };
    // Prefer whole-URL transformations: preserve every query parameter and hash.
    const wholeValues = [['url', url.href], ['urlNoScheme', url.href.slice(url.protocol.length + 2)], ['pathAndQuery', url.pathname + url.search + url.hash]];
    if (hashRoute) wholeValues.push(['hashPathAndQuery', hashRoute.pathAndQuery]);
    for (const [token, value] of wholeValues) {
      if (value.length < 2) continue;
      for (const encoded of [false, true]) {
        const candidate = encoded ? encodeURIComponent(value) : value;
        const index = appUrl.indexOf(candidate, appUrl.indexOf(':') + 1);
        if (index < 0 || appUrl.indexOf(candidate, index + candidate.length) >= 0) continue;
        const after = appUrl[index + candidate.length];
        if (after && !'/?&#'.includes(after)) continue;
        rule.target = appUrl.slice(0, index) + `{${token}${encoded ? '|encode' : ''}}` + appUrl.slice(index + candidate.length);
        rule.match.pathname = token === 'hashPathAndQuery' ? url.pathname : sectionPattern(url.pathname, parts);
        if (hashRoute) rule.match.hashPathname = sectionPattern(hashRoute.pathname, hashParts);
        rule.match.hashRequired = url.hash.length > 1;
        return { rule: R.validateRule(rule), note: 'Домен и раздел фиксированы. Остаток адреса, параметры и фрагмент подставляются из новой ссылки.' };
      }
    }
    const candidates = [];
    function pathCandidate(value, index, inHash = false) {
      // A leading '+' is a fixed marker in invitation-style links. Prefer the
      // original whole segment whenever the application itself includes it.
      let prefix = '';
      if (value.startsWith('+') && value.length > 1 && !appUrl.includes(value) && !appUrl.includes(encodeURIComponent(value))) {
        prefix = '+'; value = value.slice(1);
      }
      const variable = prefix ? `code${index}` : `p${index}`;
      const token = prefix ? `${inHash ? 'hashCapture' : 'capture'}:${variable}` : `${inHash ? 'hashPath' : 'path'}:${index}`;
      return { value, prefix, variable, token, ...(inHash ? { hashPart: index } : { part: index }) };
    }
    // The first path segment is normally a section (/rooms, /join, /staff).
    parts.forEach((value, index) => {
      if (value && (index > 0 || parts.length === 1)) candidates.push(pathCandidate(value, index));
    });
    hashParts?.forEach((value, index) => {
      if (value && (index > 0 || hashParts.length === 1)) candidates.push(pathCandidate(value, index, true));
    });
    for (const key of new Set(url.searchParams.keys())) {
      const value = R.rawQuery(url, key);
      if (value) candidates.push({ value, token: `query:${key}`, query: key });
    }
    if (url.hash.length > 1) candidates.push({ value: url.hash.slice(1), token: 'fragment', fragment: true });
    const counts = new Map();
    candidates.forEach(item => counts.set(item.value, (counts.get(item.value) || 0) + 1));
    const used = [];
    let chunks = [{ literal: appUrl }];
    for (const item of candidates.sort((a, b) => b.value.length - a.value.length)) {
      if (counts.get(item.value) !== 1) continue; // One pair cannot disambiguate equal IDs.
      for (const encoded of [false, true]) {
        const value = encoded ? encodeURIComponent(item.value) : item.value;
        if (encoded && value === item.value) continue;
        chunks = chunks.flatMap(chunk => {
          if (!chunk.literal) return [chunk];
          const input = chunk.literal;
          let start = 0, position = 0, searchFrom = 0;
          const output = [];
          while ((position = input.indexOf(value, searchFrom)) >= 0) {
            const before = input[position - 1], after = input[position + value.length];
            // Never replace substrings of another identifier, or the scheme.
            if ((before && !'/=?&#:'.includes(before)) || (after && !'/=?&#:'.includes(after)) || (input === appUrl && position <= appUrl.indexOf(':'))) {
              searchFrom = position + value.length;
              continue;
            }
            output.push({ literal: input.slice(start, position) }, { token: `{${item.token}${encoded ? '|encode' : ''}}` });
            used.push(item); start = position + value.length; searchFrom = start;
          }
          output.push({ literal: input.slice(start) });
          return output;
        });
      }
    }
    if (used.length) {
      const path = [...parts];
      const hashPath = hashParts && [...hashParts];
      for (const item of used) {
        if (item.part !== undefined) path[item.part] = `${item.prefix}{${item.variable}}`;
        if (item.hashPart !== undefined) hashPath[item.hashPart] = `${item.prefix}{${item.variable}}`;
        if (item.query && !rule.match.queryKeys.includes(item.query)) rule.match.queryKeys.push(item.query);
        if (item.fragment) rule.match.hashRequired = true;
      }
      rule.match.pathname = '/' + path.join('/');
      if (hashPath && used.some(item => item.hashPart !== undefined)) {
        rule.match.hashPathname = '/' + hashPath.join('/');
        rule.match.hashRequired = true;
      }
      rule.target = chunks.map(chunk => chunk.token || chunk.literal).join('');
      return { rule: R.validateRule(rule), note: 'Совпадающие идентификаторы заменены переменными. Проверьте результат на другом адресе перед сохранением.' };
    }
    rule.match.exactUrl = url.href;
    return { rule: R.validateRule(rule), note: 'Однозначная подстановка не найдена. Предложение привязано к исходному URL. Для сохранения с тестовыми примерами обобщите шаблон вручную и удалите match.exactUrl.' };
  }
  globalThis.RuleInference = Object.freeze({ infer });
})();
