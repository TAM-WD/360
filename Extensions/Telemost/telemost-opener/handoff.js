"use strict";
document.querySelector("#close").addEventListener("click", async () => {
  const tab = await chrome.tabs.getCurrent();
  if (tab) await chrome.tabs.remove(tab.id);
});
chrome.runtime.sendMessage({ type: "handoff-ready", token: location.hash.slice(1) })
  .then(result => {
    if (result?.error) {
      document.querySelector("#status").textContent = result.error;
    } else if (result?.kept) {
      document.querySelector("#status").textContent = "Запрос на открытие отправлен. Подтвердите запуск приложения, если браузер спросит. После этого эту вкладку можно закрыть.";
    }
    document.querySelector("#close").hidden = false;
  }).catch(() => {
    document.querySelector("#status").textContent = "Не удалось обработать ссылку. Обновите расширение и повторите переход.";
    document.querySelector("#close").hidden = false;
  });
