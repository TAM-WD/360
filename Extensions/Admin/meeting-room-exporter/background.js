chrome.action.onClicked.addListener((tab) => {
  const target = new URL(chrome.runtime.getURL('export.html'));
  if (Number.isInteger(tab.id)) target.searchParams.set('tab', String(tab.id));
  chrome.tabs.create({url: target.href});
});
