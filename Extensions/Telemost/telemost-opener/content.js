(() => {
  "use strict";

  function getAppUrl(event) {
    // composedPath also finds anchors inside an open shadow root and their
    // nested icons/text. Delegation covers dynamically inserted links.
    for (const node of event.composedPath()) {
      if (!(node instanceof Element)) continue;
      if (node.localName !== "a" && node.localName !== "area") continue;

      const href = node.getAttribute("href") ??
        node.getAttributeNS("http://www.w3.org/1999/xlink", "href");
      if (!href) continue;

      return TelemostRouter.toAppUrl(href, node.baseURI);
    }
    return null;
  }

  function isActivation(event) {
    return event.isTrusted && (event.button === 0 || event.button === 1);
  }

  function suppressEarlyHandlers(event) {
    if (!isActivation(event) || !getAppUrl(event)) return;
    // Some sites open links on mouse/pointer down or up instead of click.
    // Keep normal left-button focus and text selection, but stop page handlers.
    event.stopImmediatePropagation();
    if (event.type === "mousedown" && event.button === 1) event.preventDefault();
  }

  function openApp(event) {
    if (!isActivation(event)) return;
    if (event.type === "click" && event.button !== 0) return;
    if (event.type === "auxclick" && event.button !== 1) return;

    const appUrl = getAppUrl(event);
    if (!appUrl) return;

    event.preventDefault();
    event.stopImmediatePropagation();

    // Synchronous navigation retains the browser's user activation. An external
    // protocol does not replace this document or create a tab. The URL was
    // formatted for the selected route above; keep its query/fragment intact.
    window.location.assign(appUrl);
  }

  const options = { capture: true, passive: false };
  for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup"]) {
    window.addEventListener(type, suppressEarlyHandlers, options);
  }
  window.addEventListener("click", openApp, options);
  window.addEventListener("auxclick", openApp, options);
})();
