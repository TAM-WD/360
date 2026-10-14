(() => {
  'use strict';
  const filename = 'user-rules.json';
  async function directory() {
    if (!globalThis.navigator?.storage?.getDirectory) throw new Error('Браузер не поддерживает файловое хранилище пользовательских правил.');
    return navigator.storage.getDirectory();
  }
  async function read() {
    const root = await directory();
    let handle;
    try { handle = await root.getFileHandle(filename); }
    catch (error) { if (error.name === 'NotFoundError') return null; throw error; }
    const file = await handle.getFile();
    if (file.size > 1024 * 1024) throw new Error('Файл пользовательских правил превышает 1 МБ.');
    return ConfigCore.rulesFile(JSON.parse(await file.text()), true);
  }
  async function write(input) {
    const document = ConfigCore.rulesFile(input, true);
    const text = JSON.stringify(document, null, 2) + '\n';
    if (new TextEncoder().encode(text).length > 1024 * 1024) throw new Error('Файл пользовательских правил превышает 1 МБ.');
    const root = await directory();
    let handle, created = false, stream;
    try { handle = await root.getFileHandle(filename); }
    catch (error) {
      if (error.name !== 'NotFoundError') throw error;
      handle = await root.getFileHandle(filename, { create: true }); created = true;
    }
    // Changes become visible only after close; abort leaves the previous file.
    try { stream = await handle.createWritable(); await stream.write(text); await stream.close(); }
    catch (error) {
      if (stream) await stream.abort().catch(() => {});
      if (created) await root.removeEntry(filename).catch(() => {});
      throw error;
    }
    return document;
  }
  globalThis.UserRulesFile = Object.freeze({ read, write, filename });
})();
