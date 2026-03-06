function injectScript() {
  const script = document.createElement("script");
  script.src = chrome.runtime.getURL("injected.js");
  script.onload = () => script.remove();
  (document.head || document.documentElement).appendChild(script);
}

injectScript();

const TIME_BUTTON_ID = "custom-time-filter-btn";
const TIME_POPUP_ID  = "custom-time-filter-popup";

function init() {
  const observer = new MutationObserver(() => handleDateFilterChange());
  observer.observe(document.body, { childList: true, subtree: true });
  handleDateFilterChange();
}

if (document.body) {
  init();
} else {
  document.addEventListener("DOMContentLoaded", init);
}

function handleDateFilterChange() {
  const dateFilterBtn = document.querySelector('[data-testid="date-filter"]');
  if (!dateFilterBtn) return;

  const dateMode = getDateMode(dateFilterBtn);

  if (dateMode !== null) {
    injectTimeFilterButton(dateFilterBtn, dateMode);
  } else {
    removeTimeFilterButton();
  }
}

function getDateMode(dateFilterBtn) {
  const spans = dateFilterBtn.querySelectorAll(".Button2-Text span");

  for (const span of spans) {
    const text = span.textContent.trim();

    if (text === "Сегодня") {
      return { type: "today" };
    }

    const match = text.match(/^(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})$/);
    if (match) {
      const [, dateFrom, dateTo] = match;
      if (dateFrom === dateTo) {
        const parsed = parseDMY(dateFrom);
        if (!parsed) return null;

        const today = new Date();
        const isToday =
          parsed.getFullYear() === today.getFullYear() &&
          parsed.getMonth()    === today.getMonth()    &&
          parsed.getDate()     === today.getDate();

        return { type: "single", date: parsed, isToday };
      }
    }
  }

  return null;
}

function parseDMY(str) {
  const [dd, mm, yyyy] = str.split(".").map(Number);
  if (!dd || !mm || !yyyy) return null;
  const d = new Date(yyyy, mm - 1, dd);
  return isNaN(d.getTime()) ? null : d;
}

function injectTimeFilterButton(dateFilterBtn, dateMode) {
  const existing = document.getElementById(TIME_BUTTON_ID);
  if (existing) {
    existing._dateMode = dateMode;
    return;
  }

  const wrapper = dateFilterBtn.closest("div");
  if (!wrapper) return;

  const newWrapper = document.createElement("div");
  newWrapper.className = wrapper.className;

  const btn = document.createElement("button");
  btn.id           = TIME_BUTTON_ID;
  btn.type         = "button";
  btn.className    = dateFilterBtn.className;
  btn.autocomplete = "off";
  btn._dateMode    = dateMode;

  btn.innerHTML = `
    <span class="Button2-Icon Button2-Icon_side_left">
      <svg role="none" width="16" height="16" viewBox="0 0 16 16"
           fill="none" xmlns="http://www.w3.org/2000/svg">
        <circle cx="8" cy="8" r="6.5" stroke="currentColor" stroke-width="1.5"/>
        <path d="M8 5V8.5L10.5 10" stroke="currentColor"
              stroke-width="1.5" stroke-linecap="round"/>
      </svg>
    </span>
    <span class="Button2-Text">
      <div class="S2za5ih8-nT9ruh1AAIBcQ==">
        <span id="${TIME_BUTTON_ID}-label"
              class="Text Text_color_primary Text_weight_medium Text_typography_body-long-m">
          Время
        </span>
      </div>
    </span>
  `;

  btn.addEventListener("click", (e) => {
    e.stopPropagation();
    toggleTimePopup(btn);
  });

  newWrapper.appendChild(btn);
  wrapper.insertAdjacentElement("afterend", newWrapper);

  const from = sessionStorage.getItem("ctp_from");
  const to   = sessionStorage.getItem("ctp_to");
  if (from && to) updateButtonLabel(from, to);
}

function removeTimeFilterButton() {
  document.getElementById(TIME_BUTTON_ID)?.closest("div")?.remove();
  removePopup();
  sessionStorage.removeItem("ctp_from");
  sessionStorage.removeItem("ctp_to");
}

function toggleTimePopup(anchorBtn) {
  if (document.getElementById(TIME_POPUP_ID)) { removePopup(); return; }
  showPopup(anchorBtn);
}

function getNowHM() {
  const n = new Date();
  return { h: n.getHours(), m: n.getMinutes() };
}

function fmt(h, m) {
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}

function isLimitedByNow(dateMode) {
  return (
    dateMode.type === "today" ||
    (dateMode.type === "single" && dateMode.isToday)
  );
}

function showPopup(anchorBtn) {
  const popup = document.createElement("div");
  popup.id    = TIME_POPUP_ID;

  const dateMode  = anchorBtn._dateMode || { type: "today" };
  const savedFrom = sessionStorage.getItem("ctp_from") || "";
  const savedTo   = sessionStorage.getItem("ctp_to")   || "";
  const { h, m }  = getNowHM();
  const limited   = isLimitedByNow(dateMode);
  const defaultTo = limited ? fmt(h, m) : "23:59";

  const hintHTML = limited
    ? `<div class="ctp-now-hint">Сейчас: <span>${fmt(h, m)}</span></div>`
    : `<div class="ctp-now-hint">День: <span>${dateMode.date.toLocaleDateString("ru-RU")}</span></div>`;

  popup._dateMode = dateMode;

  popup.innerHTML = `
    <div class="ctp-popup__title">
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none"
           xmlns="http://www.w3.org/2000/svg">
        <circle cx="8" cy="8" r="6.5" stroke="white" stroke-width="1.5"/>
        <path d="M8 5V8.5L10.5 10" stroke="white"
              stroke-width="1.5" stroke-linecap="round"/>
      </svg>
      Диапазон времени
    </div>
    ${hintHTML}
    <div class="ctp-popup__row">
      <label class="ctp-popup__label">От</label>
      <input class="ctp-popup__input" type="time" id="ctp-time-from"
             value="${savedFrom || "00:00"}" step="60"/>
    </div>
    <div class="ctp-popup__row">
      <label class="ctp-popup__label">До</label>
      <input class="ctp-popup__input" type="time" id="ctp-time-to"
             value="${savedTo || defaultTo}" step="60"
             ${limited ? `max="${fmt(h, m)}"` : ""}/>
    </div>
    <div class="ctp-popup__divider"></div>
    <div class="ctp-popup__error" id="ctp-error"></div>
    <div class="ctp-popup__actions">
      <button class="ctp-popup__btn ctp-popup__btn--apply" id="ctp-apply">Применить</button>
      <button class="ctp-popup__btn ctp-popup__btn--reset" id="ctp-reset">Сбросить</button>
    </div>
  `;

  document.body.appendChild(popup);
  positionPopup(popup, anchorBtn);

  if (limited) {
    const toInput = popup.querySelector("#ctp-time-to");
    toInput.addEventListener("change", () => {
      const { h: nh, m: nm } = getNowHM();
      const nowStr = fmt(nh, nm);
      if (toInput.value > nowStr) {
        toInput.value = nowStr;
        popup.querySelector("#ctp-error").textContent =
          `Время «До» не может быть позже текущего (${nowStr})`;
      } else {
        popup.querySelector("#ctp-error").textContent = "";
      }
    });
  }

  setTimeout(() => document.addEventListener("click", outsideClickHandler), 0);

  popup.querySelector("#ctp-apply").addEventListener("click", () => applyTimeRange(popup));
  popup.querySelector("#ctp-reset").addEventListener("click", () => {
    sessionStorage.removeItem("ctp_from");
    sessionStorage.removeItem("ctp_to");
    resetButtonLabel();
    removePopup();
  });
}

function positionPopup(popup, anchorBtn) {
  const rect = anchorBtn.getBoundingClientRect();
  popup.style.top  = `${rect.bottom + window.scrollY + 8}px`;
  popup.style.left = `${rect.left + window.scrollX}px`;
}

function removePopup() {
  document.getElementById(TIME_POPUP_ID)?.remove();
  document.removeEventListener("click", outsideClickHandler);
}

function outsideClickHandler(e) {
  const popup = document.getElementById(TIME_POPUP_ID);
  const btn   = document.getElementById(TIME_BUTTON_ID);
  if (popup && !popup.contains(e.target) && !btn?.contains(e.target)) removePopup();
}

function applyTimeRange(popup) {
  const from    = popup.querySelector("#ctp-time-from").value;
  const to      = popup.querySelector("#ctp-time-to").value;
  const errorEl = popup.querySelector("#ctp-error");
  const mode    = popup._dateMode;

  if (!from || !to) {
    errorEl.textContent = "Заполните оба поля";
    return;
  }

  if (isLimitedByNow(mode)) {
    const { h, m } = getNowHM();
    const nowStr = fmt(h, m);
    if (to > nowStr) {
      errorEl.textContent = `Время «До» не может быть позже текущего (${nowStr})`;
      return;
    }
  }

  if (from >= to) {
    errorEl.textContent = "Время «От» должно быть меньше «До»";
    return;
  }

  errorEl.textContent = "";
  sessionStorage.setItem("ctp_from", from);
  sessionStorage.setItem("ctp_to",   to);
  updateButtonLabel(from, to);
  removePopup();
}

function updateButtonLabel(from, to) {
  const label = document.getElementById(`${TIME_BUTTON_ID}-label`);
  if (!label) return;
  label.innerHTML = `
    <span class="Text Text_color_secondary Text_weight_medium
                 Text_typography_body-long-m">Время:</span>
    <span class="Text Text_color_primary Text_weight_medium
                 Text_typography_body-long-m">${from} – ${to}</span>
  `;
}

function resetButtonLabel() {
  const label = document.getElementById(`${TIME_BUTTON_ID}-label`);
  if (label) label.textContent = "Время";
}