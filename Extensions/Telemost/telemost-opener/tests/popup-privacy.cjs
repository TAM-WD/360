const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const assert = require('node:assert/strict');
(async () => {
  const root = path.resolve(__dirname, '..');
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'app-opener-popup-'));
  let context;
  try {
    const extension = path.join(temp, 'extension'); await fs.mkdir(extension);
    for (const name of await fs.readdir(root)) if (/\.(js|json|html|css)$/.test(name)) await fs.copyFile(path.join(root,name), path.join(extension,name));
    context = await chromium.launchPersistentContext(path.join(temp,'profile'), {channel:'chromium', headless:true, ignoreDefaultArgs:['--disable-extensions'], args:[`--disable-extensions-except=${extension}`,`--load-extension=${extension}`]});
    await context.route('https://**/*', r => r.fulfill({body:'Fixture'}));
    const worker = context.serviceWorkers()[0] || await context.waitForEvent('serviceworker');
    await worker.evaluate(() => initialized);
    const id = await worker.evaluate(() => chrome.runtime.id);
    const options = await context.newPage(); await options.goto(`chrome-extension://${id}/options.html`);
    await options.locator('[data-rule-id="messenger-staff"]').waitFor();
    for (const [name, url, appUrl] of [
      ['tg chat', 'https://t.me/c/987654329/876543219', 'tg://privatepost?channel=987654329&post=876543219'],
      ['tg invite', 'https://t.me/+PrivateInvite_92873', 'tg://join?invite=PrivateInvite_92873']
    ]) {
      await options.locator('#rule-name').fill(name);
      await options.locator('#source-url').fill(url);
      await options.locator('#app-url').fill(appUrl);
      await options.locator('#analyze').click();
      await options.locator('#save-rule').click();
      await options.locator('#proposal').waitFor({state:'hidden'});
      assert.equal(await options.locator('#source-url').inputValue(), '');
    }
    const stored = await worker.evaluate(async () => ({ ...await chrome.storage.local.get(null), userRules: await UserRulesFile.read() }));
    assert.equal(stored.userSettings.rules, undefined);
    for (const value of ['987654329','876543219','PrivateInvite_92873']) assert.equal(JSON.stringify(stored).includes(value), false);
    assert.deepEqual(stored.userRules.rules[0].examples, [{url:'https://t.me/c/123456/123457',appUrl:'tg://privatepost?channel=123456&post=123457'}]);
    assert.deepEqual(stored.userRules.rules[1].examples, [{url:'https://t.me/+a1b2c3',appUrl:'tg://join?invite=a1b2c3'}]);
    assert.equal(await worker.evaluate(() => ConfigService.toAppUrl('https://t.me/+AnotherInvite')), 'tg://join?invite=AnotherInvite');
    console.log('PASS raw input absent from userSettings and effectiveConfig; actual URL substitution retained');
    await options.locator('#rules .rule').filter({hasText:'tg invite'}).getByRole('button',{name:'Изменить'}).click();
    assert.equal(await options.locator('#source-url').inputValue(), 'https://t.me/+a1b2c3');
    await options.locator('#save-rule').click();
    await options.locator('#proposal').waitFor({state:'hidden'});
    console.log('PASS reopened rule shows synthetic example and can be saved again');
    const popup = await context.newPage(); await popup.setViewportSize({width:370,height:600});
    await popup.goto(`chrome-extension://${id}/popup.html`);
    await popup.locator('[data-rule-id="messenger-staff"]').waitFor();
    assert.equal(await popup.title(),'Открыватель приложений');
    assert.equal(await popup.locator('.brand').count(),0);
    assert.deepEqual((await popup.locator('#rules strong').allTextContents()).slice(0,3), ['Телемост: встречи','Телемост: чаты','Телемост: сотрудники']);
    assert.equal(await popup.locator('#rules small').count(),0);
    assert.equal(await popup.locator('#rules').textContent().then(s=>s.includes('{')),false);
    const metrics = await popup.evaluate(() => ({gap:document.querySelector('#open-settings').getBoundingClientRect().top-document.querySelector('#rules').getBoundingClientRect().bottom,height:document.querySelector('main').getBoundingClientRect().height}));
    assert.equal(metrics.gap,12);
    assert.ok(metrics.height<450, JSON.stringify(metrics));
    await popup.setViewportSize({width:370,height:Math.ceil(metrics.height)});
    if (process.env.SCREENSHOT_PATH) await popup.screenshot({path:process.env.SCREENSHOT_PATH});
    console.log('PASS compact popup, seven rules, renamed labels, no template text; gap='+metrics.gap+'px, height='+metrics.height+'px');
    await options.locator('#source-url').fill('https://unique.example/secret-room');
    await options.locator('#app-url').fill('someapp://fixed-home');
    await options.locator('#analyze').click();
    assert.equal(await options.locator('#save-rule').isDisabled(),true);
    assert.match(await options.locator('#preview').textContent(),/точного URL/);
    console.log('PASS exact-only proposal explains why it cannot store a test example');
  } finally { if(context) await context.close(); await fs.rm(temp,{recursive:true,force:true}); }
})().catch(e=>{console.error(e);process.exitCode=1;});
