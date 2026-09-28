// Integration test: real installed extension and trusted browser input.
// Only the final OS-protocol handoff is stubbed to avoid opening test meetings.
const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const assert = require('node:assert/strict');

async function main() {
  const root = path.resolve(__dirname, '..');
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'telemost-test-'));
  const extension = path.join(temp, 'extension');
  const server = http.createServer(async (_req, res) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(await fs.readFile(path.join(root, 'test-page.html')));
  });
  let context;
  try {
    await fs.mkdir(extension);
    for (const name of ['manifest.json', 'url-router.js', 'background.js', 'handoff.html', 'handoff.js']) {
      await fs.copyFile(path.join(root, name), path.join(extension, name));
    }
    const source = await fs.readFile(path.join(root, 'content.js'), 'utf8');
    const handoff = 'window.location.assign(appUrl);';
    assert.equal(source.split(handoff).length, 2, 'exactly one native handoff');
    await fs.writeFile(path.join(extension, 'content.js'), source.replace(handoff,
      'document.documentElement.dataset.telemostTestUrl = appUrl; ' +
      'document.documentElement.dataset.telemostTestCount = String(Number(document.documentElement.dataset.telemostTestCount || 0) + 1);'));
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    });
    context = await chromium.launchPersistentContext(path.join(temp, 'profile'), {
      executablePath: process.env.CHROMIUM_PATH || undefined,
      channel: process.env.CHROMIUM_PATH ? undefined : 'chromium',
      headless: process.env.HEADED !== '1',
      timeout: 30000,
      ignoreDefaultArgs: ['--disable-extensions'],
      args: [`--disable-extensions-except=${extension}`, `--load-extension=${extension}`]
    });
    const page = context.pages()[0] || await context.newPage();
    console.log(`Browser started: ${context.browser().version()}`);
    const origin = `http://127.0.0.1:${server.address().port}`;
    let pagesCreated = 0;
    let checks = 0;
    context.on('page', () => pagesCreated++);

    async function check(name, action, suffix, targetFrame, expectedAppUrl = `telemost://${suffix}`) {
      await page.goto(origin);
      await page.waitForLoadState('load');
      await action();
      const frame = targetFrame ? targetFrame() : page;
      await frame.waitForFunction(() => document.documentElement.dataset.telemostTestCount === '1', null, { timeout: 5000 });
      assert.equal(await frame.locator('html').getAttribute('data-telemost-test-url'), expectedAppUrl);
      // Let default browser actions and synchronous site handlers settle.
      await page.waitForTimeout(100);
      assert.equal(pagesCreated, 0, 'no tab/window created, including transient ones');
      assert.equal(context.pages().length, 1);
      assert.equal(page.url(), `${origin}/`, 'source document stays open');
      assert.equal(await page.locator('#count').textContent(), '0');
      assert.equal(await frame.locator('html').getAttribute('data-telemost-test-count'), '1');
      console.log(`PASS ${name}`);
      checks++;
    }
    const plain = 'https://telemost.360.yandex.ru/j/123456789';
    const blank = 'https://telemost.360.yandex.ru/j/987654321?source=test#room';
    await check('ordinary click', () => page.locator('#regular').click(), plain);
    await check('target blank and nested element', () => page.locator('#blank strong').click(), blank);
    // macOS reserves Control+click for the native context menu (no click event).
    const modifiers = process.platform === 'darwin' ? ['Meta', 'Shift'] : ['Control', 'Meta', 'Shift'];
    for (const modifier of modifiers) {
      await check(`${modifier}+click`, () => page.locator('#blank').click({ modifiers: [modifier] }), blank);
    }
    if (process.platform === 'darwin') {
      await page.goto(origin);
      await page.locator('#blank').click({ modifiers: ['Control'] });
      assert.equal(await page.locator('html').getAttribute('data-telemost-test-url'), null);
      assert.equal(pagesCreated, 0);
      await page.keyboard.press('Escape');
      console.log('PASS macOS Control+click retains native context menu');
      checks++;
    }
    await check('middle click', () => page.locator('#blank').click({ button: 'middle' }), blank);
    await check('Enter', async () => {
      await page.locator('#blank').focus();
      await page.keyboard.press('Enter');
    }, blank);
    await check('site window.open handler blocked', () => page.locator('#scripted').click(), plain);
    await check('open shadow root', () => page.locator('#shadow a').click(), plain);
    await check('dynamic link', async () => {
      await page.locator('#add').click();
      await page.locator('#dynamic a').click();
    }, 'https://telemost.360.yandex.ru/j/123456789');
    await check('srcdoc iframe', () => page.frameLocator('iframe').locator('a').click(),
      'https://telemost.360.yandex.ru/j/123456789', () => page.frames().find(frame => frame.parentFrame()));
    await check('site pointerdown handler blocked', async () => {
      await page.locator('#regular').evaluate(link => link.addEventListener('pointerdown', () => window.open('about:blank')));
      await page.locator('#regular').click();
    }, plain);
    const legacy = 'https://telemost.yandex.ru/j/123456789?source=test#room';
    await check('legacy /j/ target blank is intercepted', () => page.locator('#legacy strong').click(), legacy);
    await check('legacy /j/ middle click creates no tab', () => page.locator('#legacy').click({ button: 'middle' }), legacy);
    await check('legacy /j/ Enter is intercepted', async () => {
      await page.locator('#legacy').focus();
      await page.keyboard.press('Enter');
    }, legacy);
    await check('legacy /j/ modifier click creates no tab', () => page.locator('#legacy').click({
      modifiers: [process.platform === 'darwin' ? 'Meta' : 'Control']
    }), legacy);
    const join = 'https://telemost.360.yandex.ru/join#room%2F123?token=A%2BB&source=test';
    const joinAppUrl = 'telemost://ychat/telemost.360.yandex.ru/join#room%2F123?token=A%2BB&source=test';
    await check('join uses telemost://ychat/ with full fragment', () => page.locator('#join strong').click(), join, null, joinAppUrl);
    await check('join middle click creates no tab', () => page.locator('#join').click({ button: 'middle' }), join, null, joinAppUrl);
    await check('join Enter uses telemost://ychat/', async () => {
      await page.locator('#join').focus();
      await page.keyboard.press('Enter');
    }, join, null, joinAppUrl);
    await check('join modifier click creates no tab', () => page.locator('#join').click({
      modifiers: [process.platform === 'darwin' ? 'Meta' : 'Control']
    }), join, null, joinAppUrl);
    const legacyJoin = 'https://telemost.yandex.ru/join#room%2F123?token=A%2BB&source=test';
    const legacyJoinAppUrl = 'telemost://ychat/telemost.yandex.ru/join#room%2F123?token=A%2BB&source=test';
    await check('legacy /join preserves host and full fragment', () => page.locator('#legacy-join strong').click(), legacyJoin, null, legacyJoinAppUrl);
    await check('legacy /join middle click creates no tab', () => page.locator('#legacy-join').click({ button: 'middle' }), legacyJoin, null, legacyJoinAppUrl);
    await check('legacy /join Enter is intercepted', async () => {
      await page.locator('#legacy-join').focus();
      await page.keyboard.press('Enter');
    }, legacyJoin, null, legacyJoinAppUrl);
    await check('legacy /join modifier click creates no tab', () => page.locator('#legacy-join').click({
      modifiers: [process.platform === 'darwin' ? 'Meta' : 'Control']
    }), legacyJoin, null, legacyJoinAppUrl);
    await page.goto(origin);
    await page.locator('#ordinary').click();
    assert.equal(page.url(), `${origin}/#control`);
    assert.equal(await page.locator('html').getAttribute('data-telemost-test-url'), null);
    console.log('PASS unrelated link retains default navigation');
    checks++;
    // Fulfil excluded HTTPS destinations locally: verify real default navigation
    // without sending a request to the meeting service.
    await page.route('https://telemost.yandex.ru/**', route => route.fulfill({ contentType: 'text/html', body: '<p>Local test response</p>' }));
    await page.route('https://telemost.360.yandex.ru/**', route => route.fulfill({ contentType: 'text/html', body: '<p>Local test response</p>' }));
    for (const href of [
      'https://telemost.yandex.ru/join',
      'https://telemost.360.yandex.ru/',
      'https://telemost.360.yandex.ru/j/',
      'https://telemost.360.yandex.ru/join',
      'https://telemost.360.yandex.ru/join/#room'
    ]) {
      await page.goto(origin);
      await page.evaluate(href => {
        const link = Object.assign(document.createElement('a'), { id: 'excluded', href, textContent: 'Excluded URL' });
        document.body.append(link);
      }, href);
      await page.locator('#excluded').click();
      await page.waitForURL(href, { timeout: 5000 });
      assert.equal(await page.locator('html').getAttribute('data-telemost-test-url'), null);
      assert.equal(pagesCreated, 0);
      console.log(`PASS excluded URL retains browser navigation: ${href}`);
      checks++;
    }
    console.log(`${checks} browser checks passed (${context.browser().version()}); native handoff stubbed.`);
  } finally {
    if (context) await context.close();
    await new Promise(resolve => server.close(resolve));
    await fs.rm(temp, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
