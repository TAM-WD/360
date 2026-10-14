// Real Chromium UI and navigation for re-analyzing a saved exact Messenger rule.
// Both content-script and background OS launch boundaries are recorded only.
const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, label) {
  for (let i = 0; i < 100; i++) {
    if (await check()) return;
    await delay(50);
  }
  throw new Error(`Timed out: ${label}`);
}
(async () => {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'telemost-hash-rules-'));
  let context, worker, options;
  let checks = 0, sourceLoads = 0;
  const pass = name => { checks++; console.log(`PASS ${name}`); };
  const web = name => `https://messenger.360.yandex.ru/#/staff/${name}`;
  const app = name => `telemost://ychat/telemost.yandex.ru/staff/${name}`;
  const count = () => worker.evaluate(() => (globalThis.launches || []).length);
  try {
    const extension = path.join(temp, 'extension');
    await fs.mkdir(extension);
    for (const file of await fs.readdir(root)) {
      if (/\.(js|json|html|css)$/.test(file)) await fs.copyFile(path.join(root, file), path.join(extension, file));
    }
    // This regression exercises a personal rule in a deployment without the
    // default Messenger route; otherwise the built-in rule owns the same scope.
    const configPath = path.join(extension, 'rules.json');
    const config = JSON.parse(await fs.readFile(configPath, 'utf8'));
    config.rules = config.rules.filter(rule => rule.id !== 'messenger-staff');
    await fs.writeFile(configPath, JSON.stringify(config));
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
      await context.route('https://**/*', route => {
        const auto = route.request().url() === 'https://source.example/auto';
        if (auto) sourceLoads++;
        return route.fulfill({ contentType: 'text/html', body: auto
          ? `<title>Redirector</title><script>setTimeout(() => location.replace(${JSON.stringify(web('lyxm3'))}), 100);</script>`
          : `<title>Fixture</title><a id="link" href="${web('lyxm2')}">Staff</a>` }).catch(() => {});
      });
      worker = context.serviceWorkers()[0] || await context.waitForEvent('serviceworker');
      await worker.evaluate(() => initialized);
      const id = await worker.evaluate(() => chrome.runtime.id);
      options = await context.newPage();
      await options.goto(`chrome-extension://${id}/options.html`);
      await options.locator('[data-rule-id="telemost-staff"]').waitFor();
    }
    const command = async value => {
      const response = await options.evaluate(command => chrome.runtime.sendMessage({ type: 'change-config', command }), value);
      assert.equal(response.error, null);
    };
    await launch();
    const example = JSON.parse(await fs.readFile(path.join(root, 'examples/messenger-staff-rule.json'), 'utf8'));
    const oldRule = structuredClone(example);
    delete oldRule.match.hashPathname;
    oldRule.match.hashRequired = false;
    oldRule.match.exactUrl = web('lyxm');
    oldRule.target = app('lyxm');
    oldRule.examples = [{ url: web('lyxm'), appUrl: app('lyxm') }];
    oldRule.enabled = false;
    // Seed a legacy profile: new exact rules can no longer store real examples.
    await worker.evaluate(rule => chrome.storage.local.set({ userSettings: { rules: [rule] } }), oldRule);
    await until(async () => await worker.evaluate(id => ConfigService.snapshot.config.rules.some(rule => rule.id === id), oldRule.id), 'legacy rule loaded');
    const card = options.locator('#rules .rule').filter({ hasText: 'mess staff' });
    await card.getByRole('button', { name: 'Изменить' }).click();
    await options.locator('#try-url').fill(web('lyxm2'));
    assert.match(await options.locator('#preview').textContent(), /точного URL/);
    pass('saved exact rule reproduces the reported failure before re-analysis');
    await options.locator('#analyze').click();
    await options.locator('#try-url').fill(web('lyxm2'));
    assert.equal(await options.locator('#preview').textContent(), app('lyxm2'));
    assert.match(await options.locator('#scope').textContent(), /#\/staff\/\{tail\}/);
    const proposal = JSON.parse(await options.locator('#rule-json').inputValue());
    assert.equal(proposal.match.exactUrl, undefined);
    assert.equal(proposal.id, oldRule.id);
    assert.equal(proposal.name, 'mess staff');
    assert.equal(proposal.enabled, false);
    pass('re-analysis creates a variable and preserves rule identity, name and disabled state');
    await options.locator('#try-url').fill('https://messenger.360.yandex.ru/#/chats/lyxm2');
    assert.equal(await options.locator('#preview').textContent(), 'Эта ссылка не соответствует правилу.');
    pass('other hash-route sections remain outside the rule');
    await options.locator('#save-rule').click();
    await until(async () => await worker.evaluate(id => ConfigService.snapshot.config.rules.find(r => r.id === id)?.match.hashPathname === '/staff/{tail}', oldRule.id), 'updated rule saved');
    assert.equal(await options.locator('#rules .rule').count(), 4);
    await options.locator(`[data-rule-id="${oldRule.id}"]`).check();
    await until(async () => await worker.evaluate(url => ConfigService.toAppUrl(url), web('lyxm2')) === app('lyxm2'), 'rule enabled');
    const source = context.pages()[0];
    await source.goto('https://source.example/');
    // Wait for the asynchronously loaded content settings before trusted clicks.
    await source.waitForSelector('html[data-settings-ready="yes"]');
    await source.locator('#link').click();
    await until(async () => await source.locator('html').getAttribute('data-launch') === app('lyxm2'), 'variable click');
    assert.equal(source.url(), 'https://source.example/');
    pass('trusted click for lyxm2 launches the substituted association');
    const before = await count();
    const target = await context.newPage();
    const cdp = await context.newCDPSession(target);
    await cdp.send('Page.navigate', { url: web('lyxm3'), transitionType: 'typed' }).catch(() => {});
    await until(async () => target.isClosed() && await count() === before + 1, 'typed custom URL');
    assert.equal(await worker.evaluate(() => globalThis.launches.at(-1).appUrl), app('lyxm3'));
    pass('typed URL for another user substitutes and closes its new tab');
    const fragment = await context.newPage();
    await fragment.goto('https://messenger.360.yandex.ru/');
    const beforeHash = await count();
    await fragment.evaluate(() => { location.hash = '/staff/lyxm4'; });
    await until(async () => await count() === beforeHash + 1 && await fragment.locator('#close').isVisible().catch(() => false), 'hash-only navigation');
    assert.equal(await worker.evaluate(() => globalThis.launches.at(-1).appUrl), app('lyxm4'));
    assert.match(fragment.url(), /\/handoff\.html#/);
    await fragment.close();
    pass('fragment-only navigation substitutes the changed identifier');
    const beforeAuto = await count();
    await source.goto('https://source.example/auto');
    await until(async () => await count() >= beforeAuto + 1 && await source.locator('#close').isVisible().catch(() => false), 'automatic custom staff handoff');
    const stable = source.url();
    await delay(1200);
    assert.equal(await count(), beforeAuto + 1);
    assert.equal(sourceLoads, 1);
    assert.equal(source.url(), stable);
    pass('custom Messenger-to-staff redirect also avoids the restoration loop');
    const downloadPromise = options.waitForEvent('download');
    await options.locator('#export-user-rules').click();
    const download = await downloadPromise;
    const exported = JSON.parse(await fs.readFile(await download.path(), 'utf8'));
    const saved = exported.rules.find(r => r.id === oldRule.id);
    assert.equal(saved.match.hashPathname, '/staff/{tail}');
    assert.equal(saved.target, example.target);
    await command({ type: 'remove-custom', id: oldRule.id });
    await command({ type: 'import-file', document: exported });
    assert.equal(await worker.evaluate(url => ConfigService.toAppUrl(url), web('lyxm5')), app('lyxm5'));
    pass('JSON export/import retains the reusable hash-route rule');
    await context.close(); context = null;
    await launch();
    assert.equal(await worker.evaluate(url => ConfigService.toAppUrl(url), web('lyxm6')), app('lyxm6'));
    pass('hash-route substitution survives a browser restart');
    await options.locator(`[data-rule-id="${oldRule.id}"]`).uncheck();
    await until(async () => await worker.evaluate(url => ConfigService.toAppUrl(url), web('lyxm2')) === null, 'rule disabled');
    const disabled = context.pages()[0];
    await disabled.goto('https://source.example/');
    await disabled.waitForSelector('html[data-settings-ready="yes"]');
    await disabled.locator('#link').click();
    await disabled.waitForURL(web('lyxm2'));
    assert.equal(await count(), 0);
    pass('disabled custom rule preserves ordinary web navigation');
    console.log(`${checks} hash-rule checks passed (${context.browser().version()}); native handoff stubbed.`);
  } finally {
    if (context) await context.close();
    await fs.rm(temp, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
