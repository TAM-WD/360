// Run against an isolated DEMO_MODE=true server; requires the Playwright npm package.
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { chromium } = require('playwright');
const { mkdir } = require('node:fs/promises');
const path = require('node:path');

const base = process.env.TEST_BASE_URL || 'http://127.0.0.1:8016';
let browser;
before(async () => {
  browser = await chromium.launch({ headless: true, executablePath: process.env.TEST_BROWSER_PATH || undefined });
});
after(async () => { await browser?.close(); });

async function scenario(t, options = {}) {
  const context = await browser.newContext({ viewport: { width: 1365, height: 1000 }, locale: 'en-US' });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(async () => {
    await context.close();
    assert.deepEqual(errors, []);
  });
  if (options.mockSlow) {
    await page.addInitScript(() => {
      const original = window.fetch.bind(window);
      window.pendingSearches = {};
      window.fetch = (url, options) => {
        if (url === '/api/resource-lookups') {
          const resource = JSON.parse(options.body).resource_url;
          const key = resource.split('/').pop();
          if (key.startsWith('slow-')) {
            // Deliberately ignore abort: stale or stalled transports must not lock the UI.
            const pending = new Promise(resolve => { window.pendingSearches[key] = resolve; });
            if (key === 'slow-body') return Promise.resolve({ ok: true, json: () => pending });
            return pending;
          }
        }
        return original(url, options);
      };
    });
  }
  await page.goto(base);
  assert.ok(await page.locator('.mode-tag').count(), 'Use an isolated demo server, not production');
  return page;
}
async function search(page, key) {
  await page.locator('#resource-url').fill(`https://disk.yandex.ru/d/${key}`);
  await page.locator('#lookup-button').click();
}
async function waitForOwner(page) {
  await page.locator('#result').waitFor({ state: 'visible' });
  assert.equal(await page.locator('#owner-name').innerText(), 'Соколова Анна');
  assert.equal(await page.locator('#lookup-button').isEnabled(), true);
  assert.equal(await page.locator('#lookup-progress').isVisible(), false);
}
async function settleOld(page, key) {
  await page.evaluate(key => {
    window.pendingSearches[key]({ ok: false, json: async () => ({ error: { message: 'Устаревший ответ' } }) });
  }, key);
  await page.evaluate(() => new Promise(resolve => setTimeout(resolve, 0)));
}

test('English browser: invalid or empty URL gets a Russian inline error without a request', async t => {
  const page = await scenario(t);
  let requests = 0;
  page.on('request', request => { if (request.url().endsWith('/api/resource-lookups')) requests++; });
  for (const value of ['not-a-link', 'это не ссылка', '']) {
    await page.locator('#resource-url').fill(value);
    await page.locator('#lookup-button').click();
    await page.locator('#lookup-error').waitFor({ state: 'visible' });
    assert.match(await page.locator('#lookup-error').innerText(), /Введите полную ссылку|Вставьте ссылку/);
    assert.equal(await page.locator('#resource-url').getAttribute('aria-invalid'), 'true');
    assert.equal(await page.locator('#lookup-button').isEnabled(), true);
    assert.equal(await page.locator('#lookup-progress').isVisible(), false);
  }
  assert.equal(requests, 0);
  await search(page, 'demo-file');
  await waitForOwner(page);
});

test('360 host: owner is found and displayed URL keeps the original domain', async t => {
  const page = await scenario(t);
  const link = 'https://disk.360.yandex.ru/i/demo-file';
  await page.locator('#resource-url').fill(link);
  await page.locator('#lookup-button').click();
  await waitForOwner(page);
  assert.equal(await page.locator('#resource-link').getAttribute('href'), link);
});

test('server URL validation is visible and allows a corrected 360 link', async t => {
  const page = await scenario(t);
  await page.locator('#resource-url').fill('https://example.com/file');
  await page.locator('#lookup-button').click();
  await page.locator('#lookup-error').waitFor({ state: 'visible' });
  assert.match(await page.locator('#lookup-error').innerText(), /Нужна ссылка Яндекс Диска/);
  assert.equal(await page.locator('#resource-url').getAttribute('aria-invalid'), 'true');
  await page.locator('#resource-url').fill('https://disk.360.yandex.ru/d/demo-file');
  await page.locator('#lookup-button').click();
  await waitForOwner(page);
  assert.equal(await page.locator('#resource-url').getAttribute('aria-invalid'), null);
});

test('English browser: required name and invalid emails get Russian messages', async t => {
  const page = await scenario(t);
  await search(page, 'demo-file');
  await waitForOwner(page);
  await page.locator('#request-button').click();
  assert.match(await page.locator('#request-error').innerText(), /Укажите ваше имя/);
  await page.locator('[name=requester_name]').fill('Тест');
  await page.locator('#request-button').click();
  assert.match(await page.locator('#request-error').innerText(), /Укажите почту аккаунта/);
  await page.locator('[name=account_email]').fill('wrong-email');
  await page.locator('#request-button').click();
  assert.match(await page.locator('#request-error').innerText(), /Проверьте почту аккаунта/);
  await page.locator('[name=account_email]').fill(`validation-${Date.now()}@example.com`);
  await page.locator('[name=contact_email]').fill('wrong-email');
  await page.locator('#request-button').click();
  assert.match(await page.locator('#request-error').innerText(), /Проверьте почту для ответа/);
  await page.locator('[name=contact_email]').fill('');
  await page.locator('#request-button').click();
  await page.locator('#request-status').waitFor({ state: 'visible' });
});

test('external organisation: error followed by a successful lookup in the same page', async t => {
  const page = await scenario(t);
  await search(page, 'demo-external');
  await page.locator('#lookup-error').waitFor({ state: 'visible' });
  assert.match(await page.locator('#lookup-error').innerText(), /не найден|организации/);
  await search(page, 'demo-file');
  await waitForOwner(page);
  assert.equal(await page.locator('#lookup-error').isVisible(), false);
});

for (const [status, body, contentType] of [
  [403, JSON.stringify({ error: { message: 'Яндекс отклонил запрос' } }), 'application/json'],
  [429, JSON.stringify({ error: { message: 'Слишком много запросов' } }), 'application/json'],
  [502, '<html>Bad Gateway</html>', 'text/html'],
  [504, JSON.stringify({ error: { message: 'Яндекс Диск не ответил вовремя' } }), 'application/json'],
]) {
  test(`HTTP ${status}: display error and accept the next link`, async t => {
    const page = await scenario(t);
    await page.route('**/api/resource-lookups', route => route.fulfill({ status, body, contentType }), { times: 1 });
    await search(page, 'demo-external');
    await page.locator('#lookup-error').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#lookup-button').isEnabled(), true);
    assert.equal(await page.locator('#lookup-progress').isVisible(), false);
    await search(page, 'demo-file');
    await waitForOwner(page);
  });
}

for (const key of ['slow-headers', 'slow-body']) {
  test(`${key}: deadline frees the form even if transport ignores cancellation`, async t => {
    const page = await scenario(t, { mockSlow: true });
    await page.clock.install();
    await search(page, key);
    await page.waitForFunction(key => Boolean(window.pendingSearches[key]), key);
    await page.clock.fastForward(30001);
    await page.locator('#lookup-error').waitFor({ state: 'visible' });
    assert.match(await page.locator('#lookup-error').innerText(), /30 секунд/);
    assert.equal(await page.locator('#lookup-button').isEnabled(), true);
    await search(page, 'demo-file');
    await waitForOwner(page);
  });
}

test('cancel releases the form and a late response cannot overwrite the new result', async t => {
  const page = await scenario(t, { mockSlow: true });
  await search(page, 'slow-first');
  await page.locator('#cancel-lookup').click();
  assert.equal(await page.locator('#lookup-button').isEnabled(), true);
  assert.equal(await page.locator('#lookup-progress').isVisible(), false);
  await search(page, 'demo-file');
  await waitForOwner(page);
  await settleOld(page, 'slow-first');
  await waitForOwner(page);
  assert.equal(await page.locator('#lookup-error').isVisible(), false);
});

test('editing cancels the old search; its completion cannot clear a newer loading state', async t => {
  const page = await scenario(t, { mockSlow: true });
  await search(page, 'slow-first');
  await page.locator('#resource-url').fill('https://disk.yandex.ru/d/slow-second');
  assert.equal(await page.locator('#lookup-button').isEnabled(), true);
  await page.locator('#lookup-button').click();
  await settleOld(page, 'slow-first');
  assert.equal(await page.locator('#lookup-button').isDisabled(), true);
  assert.equal(await page.locator('#lookup-progress').isVisible(), true);
  assert.equal(await page.locator('#lookup-error').isVisible(), false);
  await page.locator('#resource-url').fill('https://disk.yandex.ru/d/demo-file');
  await page.locator('#lookup-button').click();
  await waitForOwner(page);
});

test('request submission still works after an external-resource error', async t => {
  const page = await scenario(t);
  await search(page, 'demo-external');
  await page.locator('#lookup-error').waitFor({ state: 'visible' });
  await search(page, 'demo-file');
  await waitForOwner(page);
  await page.locator('[name=requester_name]').fill('Тест браузера');
  await page.locator('[name=account_email]').fill(`browser-${Date.now()}@example.com`);
  await page.locator('#request-button').click();
  await page.locator('#request-status').waitFor({ state: 'visible' });
  assert.match(await page.locator('#status-label').innerText(), /очереди|сохранено/);
  assert.equal(await page.locator('#resource-url').isEnabled(), true);
});

test('desktop and mobile layouts: screenshots and no horizontal overflow', async t => {
  const page = await scenario(t);
  await search(page, 'demo-file');
  await waitForOwner(page);
  await page.mouse.move(0, 0);
  const screenshots = process.env.SCREENSHOT_DIR;
  if (screenshots) {
    await mkdir(screenshots, { recursive: true });
    await page.screenshot({ path: path.join(screenshots, 'desktop.png'), fullPage: true, animations: 'disabled' });
  }
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), true);
  if (screenshots) await page.screenshot({ path: path.join(screenshots, 'mobile.png'), fullPage: true, animations: 'disabled' });
});
