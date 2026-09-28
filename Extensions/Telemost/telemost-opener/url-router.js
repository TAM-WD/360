(() => {
  "use strict";
  const origins = new Set(["https://telemost.yandex.ru", "https://telemost.360.yandex.ru"]);
  function toAppUrl(value, base) {
    try {
      const url = new URL(value, base);
      if (!origins.has(url.origin) || url.username || url.password) return null;
      if (/^\/j\/[^/]/.test(url.pathname)) return `telemost://${url.href}`;
      if (url.pathname === "/join" && url.hash.length > 1) {
        return `telemost://ychat/${url.href.slice("https://".length)}`;
      }
    } catch { /* Not a supported URL. */ }
    return null;
  }
  globalThis.TelemostRouter = Object.freeze({ toAppUrl });
})();
