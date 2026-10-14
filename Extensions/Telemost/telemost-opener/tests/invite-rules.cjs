// Manual named captures, automatic invitation analysis, and browser handoff.
// All OS launches are stubbed. Telegram links are served by local fixtures.
const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, label) {
  for (let i = 0; i < 100; i++) { if (await check()) return; await delay(50); }
  throw new Error(`Timed out: ${label}`);
}
(async () => {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'telemost-invite-rules-'));
  let context, worker, options;
  let checks = 0;
  const pass = name => { checks++; console.log(`PASS ${name}`); };
  try {
    const extension = path.join(temp, 'extension'); await fs.mkdir(extension);
    for (const file of await fs.readdir(root)) {
      if (/\.(js|json|html|css)$/.test(file)) await fs.copyFile(path.join(root, file), path.join(extension, file));
    }
    const background = await fs.readFile(path.join(extension, 'background.js'), 'utf8');
    const boundary = 'await chrome.tabs.update(tabId, { url: appUrl, ...(activate ? { active: true } : {}) });';
    assert.equal(background.split(boundary).length, 2);
    await fs.writeFile(path.join(extension, 'background.js'), background.replace(boundary, '(globalThis.launches ||= []).push({tabId, appUrl});'));
    const content = await fs.readFile(path.join(extension, 'content.js'), 'utf8');
    assert.equal(content.split('window.location.assign(appUrl);').length, 2);
    await fs.writeFile(path.join(extension, 'content.js'), content.replace('window.location.assign(appUrl);', 'document.documentElement.dataset.launch = appUrl;'));
    await fs.appendFile(path.join(extension, 'content-config.js'), '\nRedirectSettings.ready.then(() => { const mark = () => { document.documentElement.dataset.settingsReady = "yes"; }; if (document.documentElement) mark(); else document.addEventListener("DOMContentLoaded", mark, { once: true }); });\n');
    const profile = path.join(temp, 'profile');
    async function launch() {
      context = await chromium.launchPersistentContext(profile, {
        executablePath: process.env.CHROMIUM_PATH || undefined,
        channel: process.env.CHROMIUM_PATH ? undefined : 'chromium', headless: true,
        ignoreDefaultArgs: ['--disable-extensions'], args: [`--disable-extensions-except=${extension}`, `--load-extension=${extension}`]
      });
      await context.route('https://**/*', route => route.fulfill({ contentType: 'text/html', body: '<title>Fixture</title><a id="link" href="https://t.me/+OtherCode_123" target="_blank">Invite</a>' }).catch(() => {}));
      worker = context.serviceWorkers()[0] || await context.waitForEvent('serviceworker');
      await worker.evaluate(() => initialized);
      const id = await worker.evaluate(() => chrome.runtime.id);
      options = await context.newPage(); await options.goto(`chrome-extension://${id}/options.html`);
      await options.locator('[data-rule-id="telemost-staff"]').waitFor();
    }
    const example = JSON.parse(await fs.readFile(path.join(root, 'examples/telegram-invite-rule.json'), 'utf8'));
    await launch();
    await options.locator('#source-url').fill(example.examples[0].url);
    await options.locator('#app-url').fill(example.examples[0].appUrl);
    await options.locator('#analyze').click();
    const inferred = JSON.parse(await options.locator('#rule-json').inputValue());
    assert.equal(inferred.match.pathname, '/+{code0}');
    assert.equal(inferred.target, 'tg://join?invite={capture:code0}');
    await options.locator('#try-url').fill('https://t.me/+OtherCode_123');
    assert.equal(await options.locator('#preview').textContent(), 'tg://join?invite=OtherCode_123');
    pass('automatic analysis creates a reusable named capture for the plus marker');
    await options.getByText('Изменить шаблон вручную', { exact: true }).click();
    const incorrect = structuredClone(example); incorrect.target = 'tg://join?invite={path:1}';
    await options.locator('#rule-json').fill(JSON.stringify(incorrect));
    assert.equal(await options.locator('#save-rule').isDisabled(), true);
    assert.match(await options.locator('#preview').textContent(), /не воспроизводит/);
    await options.locator('#rule-json').fill(JSON.stringify(example));
    await options.locator('#rule-name').fill(example.name);
    assert.equal(await options.locator('#save-rule').isEnabled(), true);
    assert.equal(await options.locator('#preview').textContent(), 'tg://join?invite=OtherCode_123');
    pass('manual named rule enables Save and previews another invitation');
    await options.locator('#variable-help > summary').click();
    assert.equal(await options.locator('#variable-help tbody tr').count(), 9);
    assert.match(await options.locator('#variable-help').textContent(), /\{capture:invite\}/);
    if (process.env.SCREENSHOT_PATH) await options.locator('#variable-help').screenshot({ path: process.env.SCREENSHOT_PATH });
    pass('settings include a readable nine-variable reference and named-capture example');
    await options.locator('#save-rule').click();
    await until(async () => await worker.evaluate(id => ConfigService.snapshot.config.rules.some(rule => rule.id === id), example.id), 'rule saved');
    const source = context.pages()[0]; await source.goto('https://source.example/');
    await source.waitForSelector('html[data-settings-ready="yes"]');
    const pagesBefore = context.pages().length;
    for (const button of ['left', 'middle']) {
      await source.evaluate(() => { delete document.documentElement.dataset.launch; });
      await source.locator('#link').click({ button });
      assert.equal(await source.locator('html').getAttribute('data-launch'), 'tg://join?invite=OtherCode_123');
      assert.equal(context.pages().length, pagesBefore);
      assert.equal(source.url(), 'https://source.example/');
      pass(`${button} click substitutes the code without plus and creates no tab`);
    }
    const target = await context.newPage();
    const cdp = await context.newCDPSession(target);
    await cdp.send('Page.navigate', { url: 'https://t.me/+DifferentCode_456', transitionType: 'typed' }).catch(() => {});
    await until(() => target.isClosed(), 'typed invitation closes destination');
    assert.equal(await worker.evaluate(() => globalThis.launches.at(-1).appUrl), 'tg://join?invite=DifferentCode_456');
    pass('typed invitation substitutes a different code and closes the new tab');
    const untouched = await context.newPage();
    const before = await worker.evaluate(() => globalThis.launches.length);
    await untouched.goto('https://t.me/ordinary_user');
    assert.equal(untouched.url(), 'https://t.me/ordinary_user');
    assert.equal(await worker.evaluate(() => globalThis.launches.length), before);
    await untouched.close();
    pass('ordinary Telegram usernames are outside the invitation rule');
    const downloadPromise = options.waitForEvent('download'); await options.locator('#export-user-rules').click();
    const download = await downloadPromise;
    const exported = JSON.parse(await fs.readFile(await download.path(), 'utf8'));
    assert.equal(exported.rules.at(-1).match.pathname, '/+{invite}');
    assert.equal(exported.rules.at(-1).target, 'tg://join?invite={capture:invite}');
    for (const command of [{ type: 'remove-custom', id: example.id }, { type: 'import-file', document: exported }]) {
      const response = await options.evaluate(command => chrome.runtime.sendMessage({ type: 'change-config', command }), command);
      assert.equal(response.error, null);
    }
    assert.equal(await worker.evaluate(() => ConfigService.toAppUrl('https://t.me/+ImportedCode')), 'tg://join?invite=ImportedCode');
    pass('export/import preserves named variables and their substitution');
    await context.close(); context = null; await launch();
    assert.equal(await worker.evaluate(() => ConfigService.toAppUrl('https://t.me/+RestartCode')), 'tg://join?invite=RestartCode');
    pass('named capture works after a complete browser restart');
    await options.locator(`[data-rule-id="${example.id}"]`).uncheck();
    await until(async () => await worker.evaluate(() => ConfigService.toAppUrl('https://t.me/+DisabledCode')) === null, 'rule disabled');
    const disabled = context.pages()[0]; await disabled.goto('https://source.example/');
    await disabled.waitForSelector('html[data-settings-ready="yes"]');
    await disabled.locator('#link').evaluate(a => a.removeAttribute('target'));
    await disabled.locator('#link').click();
    await disabled.waitForURL('https://t.me/+OtherCode_123');
    assert.equal(await worker.evaluate(() => (globalThis.launches || []).length), 0);
    pass('disabling the named rule leaves ordinary browser navigation');
    console.log(`${checks} invitation checks passed (${context.browser().version()}); native handoff stubbed.`);
  } finally {
    if (context) await context.close();
    await fs.rm(temp, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
