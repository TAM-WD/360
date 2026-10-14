"use strict";
document.querySelector("#close").addEventListener("click", async () => {
  const tab = await chrome.tabs.getCurrent();
  if (tab) await chrome.tabs.remove(tab.id);
});
let currentToken;
function handoff() {
  const token = location.hash.slice(1);
  if (token === currentToken) return;
  currentToken = token;
  document.querySelector("#status").textContent = "Передаю ссылку приложению…";
  document.querySelector("#close").hidden = true;
  chrome.runtime.sendMessage({ type: "handoff-ready", token })
    .then(result => {
      if (token !== currentToken) return;
      if (result?.error) {
        document.querySelector("#status").textContent = result.error;
      } else if (result?.kept) {
        document.querySelector("#status").textContent = "Запрос на открытие отправлен. Подтвердите запуск приложения, если браузер спросит. После этого эту вкладку можно закрыть.";
      }
      document.querySelector("#close").hidden = false;
    }).catch(() => {
      if (token !== currentToken) return;
      document.querySelector("#status").textContent = "Не удалось обработать ссылку. Обновите расширение и повторите переход.";
      document.querySelector("#close").hidden = false;
    });
}
// A new intercepted URL can replace only the helper's fragment: Chromium may
// cancel the HTTPS load before committing it, keeping this same document alive.
window.addEventListener("hashchange", handoff);
handoff();
