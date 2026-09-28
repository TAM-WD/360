// macOS-only native smoke test. Registers a temporary custom scheme and receiver,
// uses an isolated Chrome profile, then unregisters and deletes the receiver.
// Does not change the telemost:// association or open real meetings.
const { chromium } = require('playwright');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');
const assert = require('node:assert/strict');
const delay = ms => new Promise(r => setTimeout(r, ms));
const lsregister = '/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister';
(async () => {
  assert.equal(process.platform, 'darwin', 'This optional test requires macOS.');
  const tmp = await fs.mkdtemp(path.join(process.cwd(), 'telemost-native-'));
  const scheme = `telemost-test-${crypto.randomBytes(6).toString('hex')}`;
  const app = path.join(tmp, 'Native Receiver.app');
  const log = path.join(os.tmpdir(), `${scheme}-received.txt`);
  let context, registered = false;
  try {
    const script = path.join(tmp, 'receiver.applescript');
    await fs.writeFile(script, `on open location receivedURL\nset outFile to open for access POSIX file "${log}" with write permission\nwrite (receivedURL & linefeed) to outFile starting at eof\nclose access outFile\nend open location\n`);
    execFileSync('/usr/bin/osacompile', ['-o', app, script]);
    const plist = path.join(app, 'Contents', 'Info.plist');
    const info = JSON.parse(execFileSync('/usr/bin/plutil', ['-convert', 'json', '-o', '-', plist]));
    info.CFBundleIdentifier = `test.codex.${scheme}`;
    info.CFBundleURLTypes = [{ CFBundleURLName: scheme, CFBundleTypeRole: 'Viewer', CFBundleURLSchemes: [scheme] }];
    info.LSUIElement = true;
    await fs.writeFile(plist, JSON.stringify(info));
    execFileSync('/usr/bin/plutil', ['-convert', 'xml1', plist]);
    execFileSync('/usr/bin/codesign', ['--force', '--sign', '-', app]);
    execFileSync(lsregister, ['-f', app]); registered = true;
    await delay(1000);
    execFileSync('/usr/bin/open', ['-g', `${scheme}://receiver-self-test`]);
    let selfTest;
    for (let i = 0; i < 50; i++) {
      selfTest = await fs.readFile(log, 'utf8').catch(() => '');
      if (selfTest) break;
      await delay(100);
    }
    assert.match(selfTest, /receiver-self-test/, 'OS receiver works before browser test');
    console.log('PASS native receiver self-test');
    await fs.writeFile(log, '');
    const extension = path.join(tmp, 'extension');
    await fs.mkdir(extension);
    const root = path.resolve(__dirname, '..');
    for (const file of ['manifest.json', 'content.js', 'url-router.js', 'background.js', 'handoff.html', 'handoff.js']) await fs.copyFile(path.join(root, file), path.join(extension, file));
    const manifest = JSON.parse(await fs.readFile(path.join(extension, 'manifest.json')));
    const key = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 }).publicKey.export({type:'spki',format:'der'});
    manifest.key = key.toString('base64');
    const id = crypto.createHash('sha256').update(key).digest('hex').slice(0,32).replace(/[0-9a-f]/g, c => String.fromCharCode(97 + parseInt(c,16)));
    await fs.writeFile(path.join(extension, 'manifest.json'), JSON.stringify(manifest));
    let bg = await fs.readFile(path.join(extension,'background.js'), 'utf8');
    // Real tabs.update and real LaunchServices. Only the scheme is renamed.
    await fs.writeFile(path.join(extension, 'background.js'), bg.replace('url: appUrl,', `url: appUrl.replace('telemost:', '${scheme}:'),`));
    const profile = path.join(tmp, 'profile');
    await fs.mkdir(path.join(profile, 'Default'), { recursive: true });
    await fs.writeFile(path.join(profile, 'Default', 'Preferences'), JSON.stringify({
      protocol_handler: { allowed_origin_protocol_pairs: {
        'https://source.example': { [scheme]: true },
        [`chrome-extension://${id}`]: { [scheme]: true }
      } }
    }));
    context = await chromium.launchPersistentContext(profile, {executablePath:process.env.CHROMIUM_PATH || undefined,channel:process.env.CHROMIUM_PATH ? undefined : 'chromium',headless:false,ignoreDefaultArgs:['--disable-extensions'],args:[`--disable-extensions-except=${extension}`,`--load-extension=${extension}`]});
    await context.route('https://**/*', route => route.fulfill({contentType:'text/html',body:'<title>Native launch test</title><button>Source</button>'}));
    const worker = context.serviceWorkers()[0] || await context.waitForEvent('serviceworker');
    await worker.evaluate(() => initialized);
    assert.equal(await worker.evaluate(() => chrome.runtime.id), id);
    const source = context.pages()[0];
    await source.goto('https://source.example/');
    await source.locator('button').evaluate((button, url) => button.onclick = () => location.href = url, `${scheme}://browser-self-test`);
    await source.locator('button').click();
    await delay(2000);
    assert.match(await fs.readFile(log, 'utf8'), /browser-self-test/);
    console.log('PASS browser receiver self-test');
    await fs.writeFile(log, '');
    const cases = [
      'https://telemost.yandex.ru/j/123?test=native#room',
      'https://telemost.360.yandex.ru/join#room%2F123',
      'https://telemost.360.yandex.ru/j/123?test=native#room',
      'https://telemost.yandex.ru/join#room%2F123'
    ];
    for (let i=0; i<cases.length; i++) {
      const target = await context.newPage();
      const cdp = await context.newCDPSession(target);
      await cdp.send('Page.navigate', {url:cases[i],transitionType:'typed'}).catch(()=>{});
      let received='';
      for(let n=0;n<100;n++) {
        received = await fs.readFile(log,'utf8').catch(()=>'');
        if(received.trim().split('\n').filter(Boolean).length >= i+1 && target.isClosed()) break;
        await delay(100);
      }
      assert.equal(target.isClosed(),true);
      const expected = cases[i].includes('/join#') ? `${scheme}://ychat/${cases[i].slice(8)}` : `${scheme}://${cases[i]}`;
      // Chromium/macOS canonicalizes an empty port in the nested https: authority.
      assert.equal(received.trim().split('\n')[i], new URL(expected).href);
      assert.equal(context.pages().length,1);
      assert.equal(source.url(),'https://source.example/');
      console.log(`PASS native handoff and destination close: ${cases[i]}`);
    }
    console.log('4 native protocol handoff checks passed; temporary scheme only.');
  } finally {
    if(context) await context.close();
    const ownProcesses = execFileSync('/bin/ps', ['-axo', 'pid=,command=']).toString().split('\n');
    for (const row of ownProcesses) {
      if (row.includes(`${app}/Contents/MacOS/`)) {
        try { process.kill(Number(row.trim().split(/\s+/)[0]), 'SIGTERM'); } catch {}
      }
    }
    if(registered) execFileSync(lsregister, ['-u',app]);
    await fs.rm(tmp,{recursive:true,force:true});
    await fs.rm(log, {force:true});
  }
})().catch(e=>{console.error(e);process.exitCode=1});
