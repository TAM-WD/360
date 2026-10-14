// Upgrade a legacy profile, then inspect real OPFS files and the three downloads.
// No real application protocols or external network requests are used.
const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
(async () => {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'app-opener-files-'));
  const extension = path.join(temp, 'extension'); const profile = path.join(temp, 'profile');
  let context, worker, options, checks = 0;
  const pass = label => { checks++; console.log('PASS '+label); };
  const rule = { id:'custom-upgrade',name:'Upgrade',enabled:true,userToggleAllowed:true,match:{origins:['https://files.example'],pathname:'/room/{tail}'},target:'testapp://room/{tail}',examples:[{url:'https://files.example/room/123456',appUrl:'testapp://room/123456'}] };
  const exact = { ...structuredClone(rule),id:'custom-old-exact',match:{origins:['https://legacy.example'],pathname:'/room/456',exactUrl:'https://legacy.example/room/456'},target:'testapp://old/456',examples:[{url:'https://legacy.example/room/456',appUrl:'testapp://old/456'}] };
  async function launch(legacy = false) {
    context = await chromium.launchPersistentContext(profile, {channel:'chromium',headless:true,ignoreDefaultArgs:['--disable-extensions'],args:[`--disable-extensions-except=${extension}`,`--load-extension=${extension}`]});
    await context.route('https://**/*', route => route.fulfill({body:'Fixture'}));
    worker = context.serviceWorkers()[0] || await context.waitForEvent('serviceworker');
    await worker.evaluate(() => initialized);
    if (!legacy && !await worker.evaluate(() => typeof ConfigService !== 'undefined')) {
      // An unpacked extension keeps its registered worker until Reload is used.
      const replacement = context.waitForEvent('serviceworker');
      await worker.evaluate(() => chrome.runtime.reload()).catch(() => {});
      worker = await replacement;
      await worker.evaluate(() => initialized);
    }
    const id = await worker.evaluate(() => chrome.runtime.id);
    if (!legacy) {
      assert.equal(await worker.evaluate(() => ConfigService.snapshot.error), null);
      options = await context.newPage(); await options.goto(`chrome-extension://${id}/options.html`);
      await options.locator('[data-rule-id="messenger-staff"]').waitFor();
    }
    return id;
  }
  async function download(button, filename) {
    const promise = options.waitForEvent('download'); await options.locator(button).click();
    const result = await promise; assert.equal(result.suggestedFilename(),filename);
    return JSON.parse(await fs.readFile(await result.path(),'utf8'));
  }
  async function readFile() {
    return worker.evaluate(async () => {
      const directory = await navigator.storage.getDirectory();
      const handle = await directory.getFileHandle('user-rules.json');
      return JSON.parse(await (await handle.getFile()).text());
    });
  }
  async function command(value) {
    const result = await options.evaluate(command => chrome.runtime.sendMessage({type:'change-config',command}), value);
    assert.equal(result.error,null); return result;
  }
  try {
    await fs.mkdir(extension);
    await fs.writeFile(path.join(extension,'manifest.json'),JSON.stringify({manifest_version:3,name:'Legacy fixture',version:'1.3.4',permissions:['storage'],background:{service_worker:'background.js'}}));
    const user = {enabled:{'telemost-j':false},rules:[rule,exact]};
    await fs.writeFile(path.join(extension,'background.js'),`const initialized = chrome.storage.local.set({userSettings:${JSON.stringify(user)}});`);
    const oldId = await launch(true);
    await context.close();context=null;
    for (const name of await fs.readdir(root)) if (/\.(js|json|html|css)$/.test(name)) await fs.copyFile(path.join(root,name),path.join(extension,name));
    // Block the native protocol boundary even if a fixture accidentally navigates.
    const bg = await fs.readFile(path.join(extension,'background.js'),'utf8');
    const boundary='await chrome.tabs.update(tabId, { url: appUrl, ...(activate ? { active: true } : {}) });';
    assert.equal(bg.split(boundary).length,2);
    await fs.writeFile(path.join(extension,'background.js'),bg.replace(boundary,'(globalThis.launches ||= []).push({tabId,appUrl});'));
    assert.equal(await launch(),oldId);
    assert.deepEqual((await readFile()).rules,[rule,exact]);
    const stored = await worker.evaluate(() => chrome.storage.local.get('userSettings'));
    assert.equal(stored.userSettings.rules,undefined);
    assert.equal(stored.userSettings.enabled['telemost-j'],false);
    assert.equal(await worker.evaluate(() => ConfigService.toAppUrl('https://legacy.example/room/456')),'testapp://old/456');
    pass('legacy profile migrates both variable and exact rules into user-rules.json without loss');
    const settings = await download('#export-config','config.json');
    const bundled = await download('#export-rules','rules.json');
    const personal = await download('#export-user-rules','user-rules.json');
    assert.equal(settings.rules,undefined);assert.equal(settings.enabled['telemost-j'],false);
    assert.equal(bundled.rules.length,5);assert.equal(bundled.allowUserChanges,undefined);
    assert.deepEqual(personal,await readFile());
    pass('three download buttons produce separate settings, bundled rules and personal rules files');
    const before = await readFile();
    await command({type:'import-file',document:{...settings,enabled:{'telemost-j':true}}});
    assert.deepEqual(await readFile(),before);
    assert.equal(await worker.evaluate(() => ConfigService.snapshot.config.rules[0].enabled),true);
    pass('importing settings preserves the personal rules file');
    await command({type:'remove-custom',id:exact.id});
    await command({type:'set-enabled',id:rule.id,enabled:false});
    assert.equal((await readFile()).rules[0].enabled,false);
    await command({type:'set-enabled',id:rule.id,enabled:true});
    const backup = await download('#export-user-rules','user-rules.json');
    await command({type:'remove-custom',id:rule.id});
    assert.deepEqual((await readFile()).rules,[]);
    await options.getByText('Импортировать JSON',{exact:true}).click();
    await options.locator('#import-json').fill(JSON.stringify(backup));
    await options.locator('#import-config').click();
    for(let i=0;i<100 && (await readFile()).rules.length!==1;i++) await delay(50);
    assert.deepEqual(await readFile(),backup);
    assert.equal(await worker.evaluate(() => ConfigService.snapshot.config.rules[0].enabled),true);
    pass('toggle, remove and import update the actual file and preserve built-in switches');
    await context.close();context=null;await launch();
    assert.deepEqual(await readFile(),backup);
    assert.equal(await worker.evaluate(() => ConfigService.toAppUrl('https://files.example/room/987')),'testapp://room/987');
    pass('file contents and substitution survive a full browser restart');
    await worker.evaluate(async () => {
      const directory=await navigator.storage.getDirectory();const handle=await directory.getFileHandle('user-rules.json');
      const stream=await handle.createWritable();await stream.write('{bad');await stream.close();
    });
    const rejected=await options.evaluate(() => chrome.runtime.sendMessage({type:'change-config',command:{type:'set-enabled',id:'telemost-j',enabled:false}}));
    assert.ok(rejected.error);
    await options.evaluate(() => SettingsUI.refresh());
    assert.equal(await options.locator('#import-fields').isDisabled(),false);
    await command({type:'import-file',document:backup});
    assert.deepEqual(await readFile(),backup);
    pass('a damaged file fails visibly and can be restored through an explicit import');
    if (process.env.SCREENSHOT_PATH) {
      await options.locator('#export-config').scrollIntoViewIfNeeded();
      await options.locator('section').filter({hasText:'Файлы настроек и правил'}).screenshot({path:process.env.SCREENSHOT_PATH});
    }
    console.log(`${checks} split-file checks passed (${context.browser().version()}); native handoff stubbed.`);
  } finally { if(context) await context.close();await fs.rm(temp,{recursive:true,force:true}); }
})().catch(error=>{console.error(error);process.exitCode=1;});
