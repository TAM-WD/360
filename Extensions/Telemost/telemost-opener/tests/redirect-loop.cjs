// Reproduce a source page that redirects to /staff/ every time it loads.
// Only the OS launch is stubbed; navigation and extension lifecycle are real.
const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(condition, label) {
  for (let i = 0; i < 100; i++) {
    if (await condition()) return;
    await delay(50);
  }
  throw new Error(`Timed out: ${label}`);
}
async function main() {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'telemost-redirect-loop-'));
  let context;
  try {
    const extension = path.join(temp, 'extension');
    await fs.mkdir(extension);
    for (const file of await fs.readdir(root)) {
      if (/\.(js|json|html|css)$/.test(file)) await fs.copyFile(path.join(root, file), path.join(extension, file));
    }
    let background = await fs.readFile(process.env.BASELINE_BACKGROUND || path.join(root, 'background.js'), 'utf8');
    const boundary = 'await chrome.tabs.update(tabId, { url: appUrl, ...(activate ? { active: true } : {}) });';
    assert.equal(background.split(boundary).length, 2);
    background = background.replace(boundary, '(globalThis.launches ||= []).push({ tabId, appUrl });');
    await fs.writeFile(path.join(extension, 'background.js'), background);
    context = await chromium.launchPersistentContext(path.join(temp, 'profile'), {
      executablePath: process.env.CHROMIUM_PATH || undefined,
      channel: process.env.CHROMIUM_PATH ? undefined : 'chromium', headless: true,
      ignoreDefaultArgs: ['--disable-extensions'],
      args: [`--disable-extensions-except=${extension}`, `--load-extension=${extension}`]
    });
    const worker = context.serviceWorkers()[0] || await context.waitForEvent('serviceworker');
    await worker.evaluate(() => initialized);
    await worker.evaluate(() => {
      globalThis.handoffMessages = [];
      chrome.runtime.onMessage.addListener((message, sender) => {
        if (message.type === 'handoff-ready') globalThis.handoffMessages.push({ token: message.token, senderUrl: sender.url, frameId: sender.frameId });
      });
    });
    let target, mode, redirectDelay;
    let sourceLoads = 0;
    let helperCommits = 0;
    context.on('page', page => page.on('framenavigated', frame => {
      if (frame === page.mainFrame() && frame.url().includes('/handoff.html#')) helperCommits++;
    }));
    for (const page of context.pages()) page.on('framenavigated', frame => {
      if (frame === page.mainFrame() && frame.url().includes('/handoff.html#')) helperCommits++;
    });
    await context.route('https://**/*', async route => {
      const isSource = route.request().url() === 'https://source.example/auto';
      if (isSource) sourceLoads++;
      const redirect = mode === 'meta'
        ? `<meta http-equiv="refresh" content="0;url=${target.replaceAll('&', '&amp;')}">`
        : `<script>setTimeout(() => location.${mode}(${JSON.stringify(target)}), ${redirectDelay});</script>`;
      await route.fulfill({ contentType: 'text/html', body: isSource
        ? `<title>Redirect source</title>${redirect}`
        : '<title>Ordinary page</title>' }).catch(() => {});
    });
    const page = context.pages()[0];
    const count = () => worker.evaluate(() => (globalThis.launches || []).length);
    const cases = process.env.BASELINE_BACKGROUND || process.env.RETRY_ONLY ? [['replace', 100, true]] : [
      ['replace', 100, true], ['assign', 0, true], ['meta', 0, false], ['replace', 650, false]
    ];
    let checks = 0;
    for (const domain of ['telemost.yandex.ru', 'telemost.360.yandex.ru']) {
      for (const [method, ms, fromParam] of cases) {
        target = `https://${domain}/staff/person@example.net?selectedUid=123${fromParam ? '&from=auto_redirect_ms' : ''}`;
        mode = method; redirectDelay = ms; sourceLoads = 0; helperCommits = 0;
        const before = await count();
        await page.goto('https://source.example/auto').catch(error => {
          if (!/interrupted|ERR_ABORTED/.test(error.message)) throw error;
        });
        if (process.env.BASELINE_BACKGROUND) {
          await delay(2000);
          const launches = await count() - before;
          console.log(JSON.stringify({ sourceLoads, launches, helperCommits }));
          assert.ok(launches >= 3, 'baseline must reproduce repeated launches');
          return;
        }
        await until(async () => await count() >= before + 1 && await page.locator('#close').isVisible().catch(() => false), 'stable handoff');
        const helper = page.url();
        await delay(1200);
        assert.equal(await count(), before + 1, 'one automatic redirect requests the app exactly once');
        assert.equal(sourceLoads, 1, 'redirect source must never reload');
        assert.equal(helperCommits, 1, 'helper must not flash/reload in a loop');
        assert.equal(page.url(), helper, 'native prompt host must stay stable');
        assert.equal(await worker.evaluate(() => globalThis.launches.at(-1).appUrl), `telemost://ychat/${domain}/staff/person@example.net`);
        assert.equal(context.pages().length, 1);
        checks++;
        console.log(`PASS one launch, no reload loop: ${domain}, ${method}, ${ms} ms, from=${fromParam}`);
      }
    }
    const beforeReload = await count();
    await page.reload();
    await until(() => page.locator('#close').isVisible(), 'reloaded helper');
    assert.equal(await count(), beforeReload, 'reloading consumed helper must not reopen the app');
    checks++;
    console.log('PASS reloading helper does not replay launch');
    const cdp = await context.newCDPSession(page);
    await cdp.send('Page.navigate', { url: target, transitionType: 'typed' }).catch(() => {});
    try { await until(async () => await count() === beforeReload + 1 && await page.locator('#close').isVisible().catch(() => false), 'explicit retry'); }
    catch (error) {
      console.log('Retry diagnostics', JSON.stringify(await worker.evaluate(async () => ({ messages: globalThis.handoffMessages.slice(-4), launches: globalThis.launches, pending: [...pending], completing: [...completing], tabs: await chrome.tabs.query({}), tasks: await chrome.storage.session.get(null) }))));
      console.log('Retry page', page.url(), await page.locator('body').textContent());
      throw error;
    }
    await cdp.detach();
    checks++;
    console.log('PASS user can explicitly open the same staff URL again');
    const retry = await context.newCDPSession(page);
    await retry.send('Page.navigate', { url: target, transitionType: 'typed' }).catch(() => {});
    await until(async () => await count() === beforeReload + 2 && await page.locator('#close').isVisible().catch(() => false), 'retry from retained helper');
    await retry.detach();
    checks++;
    console.log('PASS another manual opening works from the retained helper');
    await page.goto('https://source.example/done');
    await delay(200);
    assert.equal(page.url(), 'https://source.example/done');
    assert.equal(await count(), beforeReload + 2);
    checks++;
    console.log('PASS user can leave the handoff page normally');
    console.log(`${checks} redirect-loop checks passed (${context.browser().version()}); native handoff stubbed.`);
  } finally {
    if (context) await context.close();
    await fs.rm(temp, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
