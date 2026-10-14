// Browser UI, live config, persistence and locked-deployment tests. OS launches
// are recorded in a temporary extension copy; no real meetings are opened.
const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, label) {
  for(let i=0;i<100;i++){if(await check()) return;await delay(100);}
  throw new Error(`Timed out: ${label}`);
}
(async()=>{
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'redirect-settings-'));
  let context;
  let checks = 0;
  const pass = label => { checks++; console.log(`PASS ${label}`); };
  async function prepare(name, locked = false, perRule = false) {
    const extension=path.join(temp,name);await fs.mkdir(extension);
    for(const file of await fs.readdir(root)) if(/\.(js|html|css|json)$/.test(file)) await fs.copyFile(path.join(root,file),path.join(extension,file));
    const config=JSON.parse(await fs.readFile(path.join(extension,'config.json')));
    if(locked) config.allowUserChanges=false;
    if(perRule) {
      const file=path.join(extension,'rules.json');const doc=JSON.parse(await fs.readFile(file));doc.rules[0].userToggleAllowed=false;await fs.writeFile(file,JSON.stringify(doc));
    }
    await fs.writeFile(path.join(extension,'config.json'),JSON.stringify(config));
    const bg=await fs.readFile(path.join(extension,'background.js'),'utf8');
    const boundary='await chrome.tabs.update(tabId, { url: appUrl, ...(activate ? { active: true } : {}) });';
    assert.equal(bg.split(boundary).length,2);
    await fs.writeFile(path.join(extension,'background.js'),bg.replace(boundary,'(globalThis.launches ||= []).push({tabId, appUrl});'));
    const content=await fs.readFile(path.join(extension,'content.js'),'utf8');
    await fs.writeFile(path.join(extension,'content.js'),content.replace('window.location.assign(appUrl);','document.documentElement.dataset.launch = appUrl;'));
    return {extension, profile:path.join(temp,`${name}-profile`)};
  }
  async function launch(setup) {
    context=await chromium.launchPersistentContext(setup.profile,{
      executablePath:process.env.CHROMIUM_PATH || undefined,channel:process.env.CHROMIUM_PATH?undefined:'chromium',headless:process.env.HEADED!=='1',
      ignoreDefaultArgs:['--disable-extensions'],args:[`--disable-extensions-except=${setup.extension}`,`--load-extension=${setup.extension}`]
    });
    await context.route('https://**/*',r=>r.fulfill({contentType:'text/html',body:'<title>Fixture</title><a id="link" href="https://telemost.yandex.ru/j/123">Meeting</a>'}));
    const worker=context.serviceWorkers()[0] || await context.waitForEvent('serviceworker');
    await worker.evaluate(()=>initialized);
    const id=await worker.evaluate(()=>chrome.runtime.id);
    const options=await context.newPage();await options.goto(`chrome-extension://${id}/options.html`);
    await options.locator('[data-rule-id="telemost-staff"]').waitFor();
    return {worker,id,options};
  }
  async function command(options, command) {
    return options.evaluate(command=>chrome.runtime.sendMessage({type:'change-config',command}),command);
  }
  try {
    const setup=await prepare('editable');
    let {worker,id,options}=await launch(setup);
    const errors=[];context.on('weberror',e=>errors.push(e.error().message));
    for(const id of ['telemost-j','telemost-join','telemost-staff','messenger-staff']) assert.equal(await options.locator(`[data-rule-id="${id}"]`).isChecked(),true);
    pass('all four route switches start enabled');
    const source=context.pages()[0];await source.goto('https://source.example/');
    for(const host of ['telemost.yandex.ru','telemost.360.yandex.ru']) {
      const url=`https://${host}/staff/person@example.net?selectedUid=123&from=auto_redirect_ms`;
      await source.locator('#link').evaluate((a,url)=>{a.href=url;delete document.documentElement.dataset.launch;},url);
      await source.locator('#link').click();
      await until(async()=>await source.locator('html').getAttribute('data-launch')===`telemost://ychat/${url.slice(8)}`,'staff click');
      assert.equal(source.url(),'https://source.example/');
      pass(`staff click forwards full URL on ${host}`);
    }
    await options.locator('[data-rule-id="telemost-j"]').uncheck();
    await until(async()=>await worker.evaluate(()=>!ConfigService.snapshot.config.rules[0].enabled),'j disabled');
    await delay(150);
    await source.locator('#link').evaluate(a=>{a.href='https://telemost.yandex.ru/j/123';delete document.documentElement.dataset.launch;});
    await source.locator('#link').click();
    await source.waitForURL('https://telemost.yandex.ru/j/123');
    assert.equal(await source.locator('html').getAttribute('data-launch'),null);
    assert.equal(await worker.evaluate(()=>(globalThis.launches||[]).length),0);
    pass('switch disables clicks and background fallback immediately');
    await source.goto('https://source.example/');
    const popup=await context.newPage();await popup.goto(`chrome-extension://${id}/popup.html`);
    await popup.locator('[data-rule-id="telemost-j"]').waitFor();
    assert.equal(await popup.locator('[data-rule-id="telemost-j"]').isChecked(),false);
    await popup.locator('[data-rule-id="telemost-staff"]').uncheck();
    await until(async()=>!(await options.locator('[data-rule-id="telemost-staff"]').isChecked()),'popup option sync');
    pass('popup and settings page synchronize live');
    await options.locator('#rule-name').fill('Example app');
    await options.locator('#source-url').fill('https://meet.example/room/123?pwd=a%2Bb');
    await options.locator('#app-url').fill('exampleapp://join?id=123&password=a%2Bb');
    await options.locator('#analyze').click();
    await options.locator('#try-url').fill('https://meet.example/room/987?pwd=c%2Fd');
    assert.equal(await options.locator('#preview').textContent(),'exampleapp://join?id=987&password=c%2Fd');
    if(process.env.SCREENSHOT_PATH) await options.screenshot({path:process.env.SCREENSHOT_PATH,fullPage:true});
    await options.locator('#save-rule').click();
    await until(async()=>await options.locator('#rules .rule').count()===5,'custom saved');
    const customId=await worker.evaluate(()=>ConfigService.snapshot.config.rules.find(r=>r.id.startsWith('custom-')).id);
    pass('two inputs produce a reusable rule and preview a new identifier');
    await source.locator('#link').evaluate(a=>{a.href='https://meet.example/room/555?pwd=k%2Bl';delete document.documentElement.dataset.launch;});
    await source.locator('#link').click();
    await until(async()=>await source.locator('html').getAttribute('data-launch')==='exampleapp://join?id=555&password=k%2Bl','custom click');
    pass('saved custom rule applies to already open pages');
    const beforePages=context.pages().length;
    const target=await context.newPage();
    const cdp=await context.newCDPSession(target);await cdp.send('Page.navigate',{url:'https://meet.example/room/777?pwd=a%2Fb',transitionType:'typed'}).catch(()=>{});
    await until(()=>target.isClosed(),'custom navigation closes new tab');
    assert.equal(context.pages().length,beforePages);
    assert.equal(await worker.evaluate(()=>globalThis.launches.at(-1).appUrl),'exampleapp://join?id=777&password=a%2Fb');
    pass('custom rule also handles typed navigation and closes its tab');
    const downloadPromise=options.waitForEvent('download');await options.locator('#export-user-rules').click();
    const download=await downloadPromise;const exported=JSON.parse(await fs.readFile(await download.path(),'utf8'));
    assert.equal(exported.rules.length,1);assert.equal(exported.rules.at(-1).examples[0].appUrl,'exampleapp://join?id=123456&password=a1b2c3');
    pass('user-rules.json export contains only personal rules and synthetic examples');
    await options.locator('#rules .rule').filter({hasText:'Example app'}).getByRole('button',{name:'Удалить'}).click();
    await until(async()=>await options.locator('#rules .rule').count()===4,'delete');
    await options.getByText('Импортировать JSON',{exact:true}).click();
    await options.locator('#import-json').fill(JSON.stringify(exported));await options.locator('#import-config').click();
    await until(async()=>await options.locator('#rules .rule').count()===5,'import');
    pass('exported JSON restores a deleted rule');
    await options.locator('#import-json').fill('{ invalid');await options.locator('#import-config').click();
    await until(async()=>await options.locator('#status').getAttribute('class').then(v=>v.includes('error')),'invalid json shown');
    assert.equal(await options.locator('#rules .rule').count(),5);
    pass('invalid JSON does not overwrite saved rules');
    assert.deepEqual(errors,[]);
    await context.close();context=null;
    ({worker,id,options}=await launch(setup));
    assert.equal(await options.locator('[data-rule-id="telemost-j"]').isChecked(),false);
    assert.equal(await options.locator('[data-rule-id="telemost-staff"]').isChecked(),false);
    assert.equal(await options.locator(`[data-rule-id="${customId}"]`).isChecked(),true);
    pass('toggles and custom rules survive a full browser restart');
    await context.close();context=null;
    ({worker,id,options}=await launch(await prepare('locked',true)));
    assert.equal(await options.locator('[data-rule-id="telemost-j"]').isDisabled(),true);
    assert.equal(await options.locator('#analyze').isDisabled(),true);
    const forbidden=await command(options,{type:'set-enabled',id:'telemost-j',enabled:false});assert.match(forbidden.error,/запрещены/);
    const forbiddenImport=await command(options,{type:'import-config',config:exported});assert.match(forbiddenImport.error,/запрещены/);
    await worker.evaluate(async()=>{await chrome.storage.local.set({userSettings:{enabled:{'telemost-j':false}}});});
    await delay(200);
    assert.equal(await worker.evaluate(()=>ConfigService.snapshot.config.rules[0].enabled),true);
    pass('global admin lock disables controls and rejects direct writes/imports');
    await context.close();context=null;
    ({worker,id,options}=await launch(await prepare('partial',false,true)));
    assert.equal(await options.locator('[data-rule-id="telemost-j"]').isDisabled(),true);
    assert.equal(await options.locator('[data-rule-id="telemost-join"]').isDisabled(),false);
    assert.match((await command(options,{type:'set-enabled',id:'telemost-j',enabled:false})).error,/заблокирован/);
    await options.locator('[data-rule-id="telemost-join"]').uncheck();
    await until(async()=>await worker.evaluate(()=>!ConfigService.snapshot.config.rules[1].enabled),'unlocked join');
    pass('individual admin locks leave other route settings editable');
    console.log(`${checks} settings checks passed (${context.browser().version()}).`);
  } finally {
    if(context) await context.close();
    await fs.rm(temp,{recursive:true,force:true});
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
