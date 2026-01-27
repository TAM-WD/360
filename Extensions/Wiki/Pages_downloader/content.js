chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === 'getLinks') {
    collectAllLinksRecursively().then((links) => {
      chrome.runtime.sendMessage({
        action: 'linksCollected',
        links: links
      });
      sendResponse({ success: true, count: links.length });
    });
    return true;
  }
  
  if (request.action === 'extractPageData') {
    const pageData = extractPageData(request.url);
    sendResponse(pageData);
    return true;
  }
  
  return true;
});

async function collectAllLinksRecursively() {
  const allLinks = new Set();
  const scrollable = document.querySelector('.NavigationTreeScrollable');
  
  if (!scrollable) {
    return [];
  }
  
  scrollable.scrollTop = 0;
  await sleep(500);
  
  let iteration = 0;
  const maxIterations = 100;
  
  while (iteration < maxIterations) {
    iteration++;
    
    await expandAllVisibleSections();
    collectVisibleLinks(allLinks);
    
    const scrollStep = 100;
    const oldScrollTop = scrollable.scrollTop;
    scrollable.scrollTop += scrollStep;
    
    await sleep(400);
    
    if (scrollable.scrollTop === oldScrollTop) {
      await expandAllVisibleSections();
      collectVisibleLinks(allLinks);
      break;
    }
  }
  
  return Array.from(allLinks).sort();
}

async function expandAllVisibleSections() {
  let round = 0;
  const maxRounds = 50;
  
  while (round < maxRounds) {
    round++;
    
    const closedArrows = Array.from(document.querySelectorAll('.NavigationTree-ItemIconArrow_closed'));
    
    if (closedArrows.length === 0) {
      break;
    }
    
    let expandedThisRound = 0;
    
    for (let i = 0; i < closedArrows.length; i++) {
      const arrow = closedArrows[i];
      
      if (!document.body.contains(arrow)) continue;
      if (!arrow.classList.contains('NavigationTree-ItemIconArrow_closed')) continue;
      
      const item = arrow.closest('.NavigationTree-Item');
      if (!item) continue;
      
      const rect = item.getBoundingClientRect();
      if (rect.height === 0) continue;
      
      const iconDiv = arrow.closest('.NavigationTree-ItemIcon');
      if (iconDiv) {
        iconDiv.click();
        expandedThisRound++;
        await sleep(100);
      }
    }
    
    if (expandedThisRound === 0) {
      break;
    }
    
    await sleep(500);
  }
}

function collectVisibleLinks(allLinks) {
  const linkElements = document.querySelectorAll('a.NavigationTree-ItemTitle');
  
  linkElements.forEach(link => {
    const href = link.getAttribute('href');
    
    if (!href || href === '/' || href === '') {
      return;
    }
    
    const item = link.closest('.NavigationTree-Item');
    if (item) {
      const rect = item.getBoundingClientRect();
      if (rect.height > 0) {
        allLinks.add(href);
      }
    }
  });
}

function extractPageData(url) {
  const pageError = document.querySelector('.WikiPage-Content .PageError');
  
  if (pageError) {
    const accessRequest = pageError.querySelector('.AccessRequest');
    
    if (accessRequest) {
      const errorTitle = accessRequest.querySelector('.AccessRequest-Title');
      const reason = errorTitle ? errorTitle.textContent.trim() : 'Нет доступа к странице';
      
      const anyTitle = document.querySelector('h1');
      const title = anyTitle ? anyTitle.textContent.trim() : 'Без названия';
      
      return {
        failed: true,
        title: title,
        reason: reason
      };
    }
    
    const anyTitle = document.querySelector('h1');
    const title = anyTitle ? anyTitle.textContent.trim() : 'Без названия';
    
    return {
      failed: true,
      title: title,
      reason: 'Ошибка на странице'
    };
  }
  
  const pageDocMain = document.querySelector('.PageDoc-Main');
  
  if (!pageDocMain) {
    const clusterHomePage = document.querySelector('.ClusterHomePage');
    if (clusterHomePage) {
      const title = document.querySelector('.ClusterHomePage-Title, h1');
      return {
        href: url,
        title: title ? title.textContent.trim() : 'Главная',
        date: '',
        content: ''
      };
    }
    
    const anyTitle = document.querySelector('h1');
    const title = anyTitle ? anyTitle.textContent.trim() : 'Без названия';
    
    return {
      failed: true,
      title: title,
      reason: 'Не удалось загрузить страницу'
    };
  }
  
  const titleElement = pageDocMain.querySelector('h1.DocTitle');
  const title = titleElement ? titleElement.textContent.trim() : 'Без названия';
  
  const dateElement = pageDocMain.querySelector('.PageDoc-Updated');
  let date = '';
  if (dateElement) {
    date = dateElement.textContent.trim().replace(/^Обновлено\s+/i, '');
  }
  
  let content = '';
  const transformedHtml = document.querySelector('.TransformedHtml');
  
  if (transformedHtml) {
    const yfmContent = transformedHtml.querySelector('.yfm');
    if (yfmContent) {
      content = yfmContent.innerHTML.trim();
    } else {
      const children = Array.from(transformedHtml.children);
      content = children.map(child => child.outerHTML).join('\n');
    }
  } else {
    const wikiFormatter = document.querySelector('.WikiFormatter .wiki-doc');
    if (wikiFormatter) {
      content = wikiFormatter.innerHTML.trim();
    }
  }
  
  return {
    href: url,
    title: title,
    date: date,
    content: content
  };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}