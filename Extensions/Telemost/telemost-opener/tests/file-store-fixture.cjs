// Minimal OPFS double: staged writes commit only on close, like createWritable.
module.exports = function fileStore() {
  const files = new Map();
  const state = { files, failWrite: false };
  const root = {
    async getFileHandle(name, options = {}) {
      if (!files.has(name)) {
        if (!options.create) throw Object.assign(new Error('Missing file'), { name: 'NotFoundError' });
        files.set(name, '');
      }
      return {
        async getFile() { const text = files.get(name); return { size: Buffer.byteLength(text), text: async () => text }; },
        async createWritable() {
          let pending;
          return {
            async write(text) { if (state.failWrite) throw new Error('Disk write failed'); pending = text; },
            async close() { files.set(name, pending); },
            async abort() { pending = undefined; }
          };
        }
      };
    },
    async removeEntry(name) { files.delete(name); }
  };
  return Object.assign(state, { navigator: { storage: { getDirectory: async () => root } } });
};
