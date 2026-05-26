/**
 * Хранилище сессий выгрузки участников по tabId через chrome.storage.session.
 *
 * Все мутации сериализуются per-tab через промис-очередь — иначе параллельные
 * сообщения (например, последний MEMBERS_BATCH и сразу за ним PAGINATION_DONE)
 * читают одно и то же базовое состояние, и последний write затирает merge.
 */

const STORAGE_NS = 'sessions';

const emptySession = () => ({
  chatId: null,
  orgId: null,
  ckey: null,
  nextCursor: null,
  status: 'idle', // idle | captured | loading | done | error
  error: null,
  members: [],
  updatedAt: 0
});

const key = (tabId) => `${STORAGE_NS}:${tabId}`;
const queues = new Map(); // tabId -> Promise

const enqueue = (tabId, task) => {
  const prev = queues.get(tabId) || Promise.resolve();
  const next = prev.then(task, task);
  queues.set(tabId, next.catch(() => {}));
  return next;
};

export const SessionStore = {
  async get(tabId) {
    const k = key(tabId);
    const data = await chrome.storage.session.get(k);
    return data[k] || emptySession();
  },

  async set(tabId, session) {
    session.updatedAt = Date.now();
    await chrome.storage.session.set({ [key(tabId)]: session });
  },

  update(tabId, mutator) {
    return enqueue(tabId, async () => {
      const current = await this.get(tabId);
      const next = mutator(current) || current;
      await this.set(tabId, next);
      return next;
    });
  },

  reset(tabId) {
    return enqueue(tabId, async () => {
      const fresh = emptySession();
      await this.set(tabId, fresh);
      return fresh;
    });
  },

  emptySession
};
