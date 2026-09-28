// Real Chromium extension lifecycle. Only the OS-protocol boundary is recorded:
// no test meetings are opened. BASELINE=1 disables only destination-tab removal.
const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const fixture = 'https://source.example/';
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(condition, label) {
  for (let i = 0; i < 100; i++) {
    if (await condition()) return;
    await delay(100);
  }
  throw new Error(`Timed out: ${label}`);
}
async function main() {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'telemost-navigation-'));
  let context;
  try {
    const extension = path.join(temp, 'extension');
    await fs.mkdir(extension);
    for (const file of ['manifest.json', 'content.js', 'url-router.js', 'handoff.html', 'handoff.js']) {
      await fs.copyFile(path.join(root, file), path.join(extension, file));
    }
    let background = await fs.readFile(path.join(root, 'background.js'), 'utf8');
    const boundary = 'await chrome.tabs.update(tabId, { url: appUrl, ...(activate ? { active: true } : {}) });';
    assert.equal(background.split(boundary).length, 2);
    background = background.replace(boundary,
      'if (globalThis.failLaunch) throw new Error("Test launch failure");\n' +
      '(globalThis.launches ||= []).push({ tabId, appUrl, activate }); ' +
      'if (globalThis.holdLaunch) await new Promise(resolve => { globalThis.resumeLaunch = resolve; });');
    const cleanup = 'if (await isCurrent(task)) await chrome.tabs.remove(target.id);';
    assert.equal(background.split(cleanup).length, 2);
    if (process.env.BASELINE === '1') background = background.replace(cleanup, '// Baseline: no tab cleanup.');
    await fs.writeFile(path.join(extension, 'background.js'), background);
    context = await chromium.launchPersistentContext(path.join(temp, 'profile'), {
      executablePath: process.env.CHROMIUM_PATH || undefined,
      channel: process.env.CHROMIUM_PATH ? undefined : 'chromium',
      headless: process.env.HEADED !== '1',
      ignoreDefaultArgs: ['--disable-extensions'],
      args: [`--disable-extensions-except=${extension}`, `--load-extension=${extension}`]
    });
    const worker = context.serviceWorkers()[0] || await context.waitForEvent('serviceworker');
    await worker.evaluate(() => initialized);
    if (process.env.DEBUG_NAV === '1') await worker.evaluate(() => {
      globalThis.navTrace = [];
      for (const name of ['onBeforeNavigate', 'onCommitted', 'onReferenceFragmentUpdated', 'onErrorOccurred']) {
        chrome.webNavigation[name].addListener(d => globalThis.navTrace.push([name, d]));
      }
    });
    const errors = [];
    context.on('weberror', error => errors.push(error.error().message));
    let slow = false;
    await context.route('https://**/*', async route => {
      if (slow && route.request().url().includes('telemost.')) await delay(300);
      await route.fulfill({ contentType: 'text/html', body: '<title>Test source</title><input id="draft"><button id="open">Open selected URL</button><div id="frame"></div>' }).catch(() => {});
    });
    const source = context.pages()[0];
    await source.goto(fixture);
    const sourceId = await worker.evaluate(async url => (await chrome.tabs.query({})).find(tab => tab.url === url).id, fixture);
    const count = () => worker.evaluate(() => (globalThis.launches || []).length);
    const last = () => worker.evaluate(() => globalThis.launches.at(-1));
    let checks = 0;
    const pass = name => { checks++; console.log(`PASS ${name}`); };
    async function navigate(page, url) {
      const cdp = await context.newCDPSession(page);
      await cdp.send('Page.navigate', { url, transitionType: 'typed' }).catch(error => {
        if (!page.isClosed() && !/Target.*closed/.test(error.message)) throw error;
      });
      await cdp.detach().catch(() => {});
    }
    const cases = [
      ['https://telemost.yandex.ru/j/123?from=test#room', 'telemost://https://telemost.yandex.ru/j/123?from=test#room'],
      ['https://telemost.360.yandex.ru/j/123?from=test#room', 'telemost://https://telemost.360.yandex.ru/j/123?from=test#room'],
      ['https://telemost.yandex.ru/join#room%2F123?token=A%2BB', 'telemost://ychat/telemost.yandex.ru/join#room%2F123?token=A%2BB'],
      ['https://telemost.360.yandex.ru/join#room%2F123?token=A%2BB', 'telemost://ychat/telemost.360.yandex.ru/join#room%2F123?token=A%2BB']
    ];
    for (const [url, appUrl] of cases) {
      for (const slowResponse of [false, true]) {
        slow = slowResponse;
        const before = await count();
        const target = await context.newPage();
        await navigate(target, url);
        await until(async () => await count() === before + 1, 'new-tab launch');
        assert.deepEqual(await last(), { tabId: sourceId, appUrl, activate: true });
        if (process.env.BASELINE === '1') {
          await until(() => target.url().includes('/handoff.html#'), 'handoff document');
          assert.equal(target.isClosed(), false);
          assert.equal(context.pages().length, 2);
          console.log('BASELINE CONFIRMED: destination remains open after handoff without explicit cleanup.');
          return;
        }
        await until(() => target.isClosed(), 'new destination closes');
        assert.equal(context.pages().length, 1);
        assert.equal(source.url(), fixture);
        pass(`typed URL in new tab closes; ${slow ? 'slow' : 'fast'} response; ${url}`);
      }
    }
    slow = false;
    for (const [url, appUrl] of cases) {
      const before = await count();
      await navigate(source, url);
      try { await until(async () => await count() === before + 1 && await worker.evaluate(() => completing.size === 0), 'existing-tab launch'); }
      catch (error) {
        console.log('DIAGNOSTICS', JSON.stringify(await worker.evaluate(async () => ({ tabs: await chrome.tabs.query({}), tasks: await chrome.storage.session.get(null), trace: globalThis.navTrace })), null, 2));
        console.log('PAGE', await source.locator('body').textContent());
        throw error;
      }
      assert.equal(source.isClosed(), false);
      assert.equal(source.url(), fixture);
      assert.equal(context.pages().length, 1);
      assert.equal((await last()).appUrl, appUrl);
      pass(`typed URL in existing tab restores its page; ${url}`);
    }
    // Browser-created navigation target with opener: equivalent destination
    // lifecycle to built-in Go to. No anchor click/content interception involved.
    for (const [url, appUrl] of cases) {
      const before = await count();
      await source.locator('#open').evaluate((button, url) => button.onclick = () => window.open(url, '_blank'), url);
      let target;
      const opened = context.waitForEvent('page').then(page => { target = page; });
      await source.locator('#open').click();
      await opened;
      await until(async () => await count() === before + 1 && target.isClosed(), 'Go-to destination closes');
      assert.equal((await last()).appUrl, appUrl);
      assert.equal((await last()).tabId, sourceId);
      assert.equal(context.pages().length, 1);
      pass(`browser-created target closes; ${url}`);
    }
    // A selected URL in an iframe does not hijack the whole top-level tab.
    const beforeFrame = await count();
    await source.evaluate(url => { const frame = document.createElement('iframe'); frame.src = url; document.body.append(frame); }, cases[0][0]);
    await until(() => source.frames().some(frame => frame.url() === cases[0][0]), 'subframe loads');
    await delay(300);
    assert.equal(await count(), beforeFrame);
    assert.equal(source.url(), fixture);
    pass('subframe navigation ignored');
    // Unrelated and incomplete Telemost URLs stay ordinary web navigation.
    for (const url of ['https://example.com/keep', 'https://telemost.yandex.ru/j/', 'https://telemost.360.yandex.ru/join']) {
      const before = await count();
      const target = await context.newPage();
      await target.goto(url);
      await delay(200);
      assert.equal(target.url(), url);
      assert.equal(await count(), before);
      await target.close();
      pass(`unmatched navigation unchanged: ${url}`);
    }
    // Completing /join via a fragment-only navigation must be intercepted too.
    const fragmentPage = await context.newPage();
    await fragmentPage.goto('https://telemost.yandex.ru/join');
    const beforeFragment = await count();
    await fragmentPage.evaluate(() => { location.hash = 'fragment-test'; });
    await until(async () => await count() === beforeFragment + 1 && await worker.evaluate(() => completing.size === 0), 'fragment launch');
    assert.equal(fragmentPage.url(), 'https://telemost.yandex.ru/join');
    assert.equal((await last()).appUrl, 'telemost://ychat/telemost.yandex.ru/join#fragment-test');
    await fragmentPage.close();
    pass('fragment-only /join navigation restores its previous page');
    // A user who leaves the helper while launch completes keeps their new page.
    await worker.evaluate(() => { globalThis.holdLaunch = true; });
    const abandoned = await context.newPage();
    const beforeAbandoned = await count();
    await navigate(abandoned, cases[0][0]);
    await until(async () => await count() === beforeAbandoned + 1, 'pending launch');
    await abandoned.goto('https://example.com/user-changed-page');
    await worker.evaluate(() => { globalThis.holdLaunch = false; globalThis.resumeLaunch(); });
    await until(async () => await worker.evaluate(() => completing.size === 0), 'old launch completes');
    assert.equal(abandoned.isClosed(), false);
    assert.equal(abandoned.url(), 'https://example.com/user-changed-page');
    await abandoned.close();
    pass('tab is not closed if the user has navigated away');
    // Pinning a blank tab opts it out of automatic closure.
    const pinned = await context.newPage();
    const pinnedId = await worker.evaluate(async () => (await chrome.tabs.query({})).find(tab => tab.url === 'about:blank').id);
    await worker.evaluate(id => chrome.tabs.update(id, { pinned: true }), pinnedId);
    const beforePinned = await count();
    await navigate(pinned, cases[0][0]);
    await until(async () => await count() === beforePinned + 1 && await pinned.locator('#close').isVisible(), 'pinned fallback');
    assert.equal(pinned.isClosed(), false);
    assert.equal((await last()).tabId, pinnedId);
    await pinned.close();
    pass('pinned tabs are retained');
    // Failure at native boundary must not remove the destination or source.
    await worker.evaluate(() => { globalThis.failLaunch = true; });
    const failed = await context.newPage();
    await navigate(failed, cases[0][0]);
    await until(async () => !failed.isClosed() && (await failed.locator('#status').textContent().catch(() => '')) === 'Test launch failure', 'launch failure visible');
    assert.equal(source.url(), fixture);
    await failed.close();
    await worker.evaluate(() => { globalThis.failLaunch = false; });
    pass('launch failure leaves destination with readable error');
    // One last tab cannot be removed without also destroying its native prompt.
    await source.close();
    const alone = await context.newPage();
    const beforeAlone = await count();
    await navigate(alone, cases[0][0]);
    await until(async () => await count() === beforeAlone + 1, 'single-tab launch');
    await until(async () => await alone.locator('#close').isVisible(), 'manual close fallback');
    assert.equal(context.pages().length, 1);
    assert.match(await alone.locator('#status').textContent(), /Подтвердите/);
    pass('last tab retains handoff page and close button');
    await alone.close();
    const blankAnchor = await context.newPage();
    const anotherBlank = await context.newPage();
    const beforeBlank = await count();
    await navigate(anotherBlank, cases[0][0]);
    await until(async () => await count() === beforeBlank + 1 && await anotherBlank.locator('#close').isVisible(), 'empty anchor fallback');
    assert.equal(blankAnchor.isClosed(), false);
    assert.equal(anotherBlank.isClosed(), false);
    pass('another empty tab is not used as the launch host');
    assert.deepEqual(errors, []);
    console.log(`${checks} navigation checks passed (${context.browser().version()}); native handoff stubbed.`);
  } finally {
    if (context) await context.close();
    await fs.rm(temp, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
