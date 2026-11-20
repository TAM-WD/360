(() => {
  "use strict";
  const $ = id => document.getElementById(id);
  let lookupId = null;
  let requestId = null;
  let pollingTimer = null;
  let pollingStarted = 0;
  let lookupBusy = false;
  let activeLookup = null;
  let requestBusy = false;
  const terminal = new Set(["accepted", "saved", "failed"]);
  const fieldNames = { resource_url: "Ссылка", requester_name: "Имя", account_email: "Почта аккаунта", contact_email: "Контактная почта", comment: "Комментарий", rights: "Права" };

  function setStep(number) {
    [1, 2, 3].forEach(n => $("step-" + n).classList.toggle("active", n <= number));
  }
  function showError(id, error) {
    $(id).textContent = error.message || "Не удалось выполнить запрос. Попробуйте ещё раз.";
    $(id).hidden = false;
    const form = $(id === "lookup-error" ? "lookup-form" : "request-form");
    let firstField = null;
    for (const name of Object.keys(error.fields || {})) {
      const field = form.elements.namedItem(name);
      if (field?.focus && field.type !== "hidden") {
        field.setAttribute("aria-invalid", "true");
        field.setAttribute("aria-describedby", id === "lookup-error" ? "resource-hint lookup-error" : "request-error");
        firstField ||= field;
      }
    }
    firstField?.focus();
  }
  function validateForm(form, errorId) {
    const requiredMessages = {
      resource_url: "Вставьте ссылку на файл или папку в Яндекс Диске.",
      requester_name: "Укажите ваше имя, чтобы владелец знал, кто запрашивает доступ.",
      account_email: "Укажите почту аккаунта Яндекса, на которую будет предоставлен доступ.",
    };
    for (const field of form.elements) {
      field.removeAttribute("aria-invalid");
    }
    for (const field of form.elements) {
      if (!field.willValidate) continue;
      const value = field.value.trim();
      let message = "";
      if (field.required && !value) {
        message = requiredMessages[field.name] || "Заполните это поле.";
      } else if (field.name === "resource_url") {
        try {
          const link = new URL(value);
          if (!["http:", "https:"].includes(link.protocol) || !link.hostname) throw new Error();
        } catch (_) {
          message = "Введите полную ссылку, начинающуюся с https://. Например: https://disk.360.yandex.ru/i/…";
        }
      } else if (field.type === "email" && value && field.validity.typeMismatch) {
        message = field.name === "contact_email"
          ? "Проверьте почту для ответа. Пример адреса: name@example.com."
          : "Проверьте почту аккаунта Яндекса. Пример адреса: name@example.com.";
      }
      if (!message && field.maxLength > 0 && field.value.length > field.maxLength) {
        message = `Сократите поле «${fieldNames[field.name] || "Текст"}» до ${field.maxLength} символов.`;
      }
      if (!message && !field.validity.valid) message = `Проверьте поле «${fieldNames[field.name] || "Значение"}».`;
      if (message) {
        showError(errorId, { message, fields: { [field.name]: [message] } });
        return false;
      }
    }
    return true;
  }
  async function api(url, data, { signal, timeoutMs = 60000, timeoutMessage } = {}) {
    const controller = new AbortController();
    let timeout;
    let cancel;
    try {
      // Race the whole operation, including the body. Abort alone must not keep the UI busy.
      const deadline = new Promise((_, reject) => {
        cancel = () => {
          reject(new DOMException("Запрос отменён", "AbortError"));
          controller.abort();
        };
        if (signal?.aborted) { cancel(); return; }
        signal?.addEventListener("abort", cancel, { once: true });
        timeout = setTimeout(() => {
          reject(new Error(timeoutMessage || "Сервис не ответил вовремя. Повторите запрос: повторное нажатие не создаст вторую заявку."));
          controller.abort();
        }, timeoutMs);
      });
      const operation = async () => {
        if (controller.signal.aborted) throw new DOMException("Запрос отменён", "AbortError");
        const response = await fetch(url, {
          method: data ? "POST" : "GET", credentials: "same-origin", signal: controller.signal,
          headers: data ? { "Content-Type": "application/json", "X-CSRFToken": document.querySelector("[name=csrfmiddlewaretoken]").value } : {},
          body: data ? JSON.stringify(data) : undefined,
        });
        const payload = await response.json().catch(() => null);
        if (!response.ok) {
          let message = payload?.error?.message || "Сервис временно недоступен. Попробуйте позже.";
          if (payload?.error?.fields) message = Object.entries(payload.error.fields).map(([k, v]) => `${fieldNames[k] || k}: ${v.join(" ")}`).join(" ");
          const error = new Error(message);
          error.fields = payload?.error?.fields;
          throw error;
        }
        if (!payload || typeof payload !== "object") throw new Error("Сервис вернул некорректный ответ. Повторите запрос.");
        return payload;
      };
      return await Promise.race([operation(), deadline]);
    } catch (error) {
      if (error instanceof TypeError) throw new Error("Нет соединения с сервисом. Проверьте сеть и повторите запрос.");
      throw error;
    } finally {
      clearTimeout(timeout);
      signal?.removeEventListener("abort", cancel);
    }
  }
  function setLookupBusy(busy) {
    lookupBusy = busy;
    $("lookup-progress").hidden = !busy;
    $("lookup-button").disabled = busy || requestBusy;
    $("lookup-form").setAttribute("aria-busy", String(busy));
  }
  function cancelLookup(message = "") {
    const previous = activeLookup;
    activeLookup = null;
    previous?.abort();
    setLookupBusy(false);
    $("lookup-feedback").textContent = message;
    $("lookup-feedback").hidden = !message;
  }
  function remember(id) {
    try { if (id) sessionStorage.setItem("disk-access-request", id); else sessionStorage.removeItem("disk-access-request"); } catch (_) { /* Storage is optional. */ }
  }
  function renderStatus(data) {
    requestId = data.request_id;
    $("request-status").hidden = false;
    $("request-status").classList.toggle("failed", data.status === "failed");
    $("status-label").textContent = data.status_label;
    $("status-message").textContent = data.message;
    $("request-number").textContent = data.request_id;
    setStep(3);
    remember(data.request_id);
    return !terminal.has(data.status);
  }
  async function pollStatus() {
    if (!requestId) return;
    const current = requestId;
    try {
      const data = await api(`/api/access-requests/${current}`);
      if (current !== requestId) return;
      if (renderStatus(data) && Date.now() - pollingStarted < 120000) pollingTimer = setTimeout(pollStatus, 3000);
    } catch (error) { if (current === requestId) $("status-message").textContent = error.message; }
  }
  $("lookup-form").addEventListener("submit", async event => {
    event.preventDefault();
    if (requestBusy) return;
    cancelLookup();
    clearTimeout(pollingTimer); requestId = null; lookupId = null; remember(null);
    $("result").hidden = true; $("request-status").hidden = true; $("lookup-error").hidden = true;
    setStep(1);
    if (!validateForm(event.currentTarget, "lookup-error")) return;
    const current = new AbortController();
    activeLookup = current;
    setLookupBusy(true);
    try {
      const data = await api("/api/resource-lookups", { resource_url: $("resource-url").value }, {
        signal: current.signal, timeoutMs: 30000,
        timeoutMessage: "Поиск не завершился за 30 секунд. Попробуйте ещё раз или укажите другую ссылку.",
      });
      if (activeLookup !== current) return;
      lookupId = data.lookup_id;
      $("owner-name").textContent = data.owner.name;
      $("owner-initial").textContent = data.owner.name.charAt(0).toUpperCase();
      $("resource-link").href = data.resource_url;
      $("resource-link").textContent = data.resource_url + " ↗";
      $("owner-warning").textContent = data.message;
      $("owner-warning").hidden = !data.message;
      $("result").hidden = false; $("request-form").hidden = !data.can_request;
      $("request-button").disabled = false; $("request-error").hidden = true;
      setStep(2);
    } catch (error) {
      if (activeLookup === current && error.name !== "AbortError") showError("lookup-error", error);
    } finally {
      if (activeLookup === current) { activeLookup = null; setLookupBusy(false); }
    }
  });
  $("cancel-lookup").addEventListener("click", () => {
    cancelLookup("Поиск отменён. Можно повторить его или вставить другую ссылку.");
    $("resource-url").focus();
  });
  $("resource-url").addEventListener("input", () => {
    if (lookupBusy) cancelLookup("Ссылка изменена. Нажмите «Найти владельца».");
    lookupId = null;
    $("result").hidden = true;
    $("lookup-error").hidden = true;
    setStep(1);
  });
  $("request-form").addEventListener("submit", async event => {
    event.preventDefault();
    if (requestBusy || lookupBusy || !lookupId) return;
    $("request-error").hidden = true;
    if (!validateForm(event.currentTarget, "request-error")) return;
    requestBusy = true; $("lookup-button").disabled = true; $("resource-url").disabled = true;
    $("request-error").hidden = true; $("request-button").disabled = true;
    const data = Object.fromEntries(new FormData(event.currentTarget));
    data.lookup_id = lookupId;
    try {
      const result = await api("/api/access-requests", data);
      $("request-form").hidden = true;
      pollingStarted = Date.now();
      if (renderStatus(result)) pollingTimer = setTimeout(pollStatus, 1000);
      $("request-status").scrollIntoView({ behavior: "smooth", block: "nearest" });
    } catch (error) { showError("request-error", error); $("request-button").disabled = false; }
    finally { requestBusy = false; $("lookup-button").disabled = false; $("resource-url").disabled = false; }
  });
  $("refresh-status").addEventListener("click", () => { clearTimeout(pollingTimer); pollingStarted = Date.now(); pollStatus(); });
  [$("lookup-form"), $("request-form")].forEach(form => form.addEventListener("input", event => {
    event.target.removeAttribute("aria-invalid");
  }));
  document.querySelectorAll("[data-example]").forEach(button => button.addEventListener("click", () => {
    if (requestBusy) return;
    $("resource-url").value = `https://disk.yandex.ru/d/${button.dataset.example}`;
    $("lookup-form").requestSubmit();
  }));
  try {
    const saved = sessionStorage.getItem("disk-access-request");
    if (saved && /^[0-9a-f-]{36}$/.test(saved)) { requestId = saved; pollingStarted = Date.now(); pollStatus(); }
  } catch (_) { /* Storage is optional. */ }
})();
