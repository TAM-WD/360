(function() {
  // Внедряем interceptor
  const interceptorScript = document.createElement('script');
  interceptorScript.src = chrome.runtime.getURL('interceptor.js');
  interceptorScript.onload = function() {
    this.remove();
  };
  (document.head || document.documentElement).appendChild(interceptorScript);

  // Внедряем navigation
  const navigationScript = document.createElement('script');
  navigationScript.src = chrome.runtime.getURL('navigation.js');
  navigationScript.onload = function() {
    this.remove();
  };
  (document.head || document.documentElement).appendChild(navigationScript);
})();