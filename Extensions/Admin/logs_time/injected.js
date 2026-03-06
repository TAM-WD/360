(function () {
  "use strict";

  const STORAGE_KEY_FROM = "ctp_from";
  const STORAGE_KEY_TO = "ctp_to";

  const _originalFetch = window.fetch;

  window.fetch = async function (...args) {
    let [input, init] = args;

    const url = typeof input === "string"
      ? input
      : input instanceof Request
        ? input.url
        : "";

    if (url.includes("/models") && init?.body) {
      try {
        const body = JSON.parse(init.body);
        const patched = patchRequestBody(body);
        if (patched) {
          init = { ...init, body: JSON.stringify(patched) };
        }
      } catch (e) {}
    }

    return _originalFetch.apply(this, [input, init]);
  };

  const _OriginalXHR = window.XMLHttpRequest;

  function PatchedXHR() {
    const xhr = new _OriginalXHR();
    let _url = "";

    const _open = xhr.open.bind(xhr);
    xhr.open = function (method, url, ...rest) {
      _url = url;
      return _open(method, url, ...rest);
    };

    const _send = xhr.send.bind(xhr);
    xhr.send = function (body) {
      if (_url.includes("/models") && body) {
        try {
          const parsed = JSON.parse(body);
          const patched = patchRequestBody(parsed);
          if (patched) {
            return _send(JSON.stringify(patched));
          }
        } catch (e) {}
      }
      return _send(body);
    };

    return xhr;
  }

  PatchedXHR.prototype = _OriginalXHR.prototype;
  window.XMLHttpRequest = PatchedXHR;

  function patchRequestBody(body) {
    const from = sessionStorage.getItem(STORAGE_KEY_FROM);
    const to = sessionStorage.getItem(STORAGE_KEY_TO);

    if (!from || !to) return null;

    const models = body?.models;
    if (!Array.isArray(models)) return null;

    let changed = false;

    models.forEach((model) => {
      const params = model?.params;
      if (!params) return;

      if (params.after_date !== undefined) {
        params.after_date = applyTimeToTimestamp(params.after_date, from, "start");
        changed = true;
      }

      if (params.before_date !== undefined) {
        params.before_date = applyTimeToTimestamp(params.before_date, to, "end");
        changed = true;
      }
    });

    return changed ? body : null;
  }

  function applyTimeToTimestamp(ts, timeStr, mode) {
    const [hours, minutes] = timeStr.split(":").map(Number);
    const date = new Date(ts);
    date.setHours(hours, minutes, mode === "start" ? 0 : 59, mode === "start" ? 0 : 999);
    return date.getTime();
  }
})();
