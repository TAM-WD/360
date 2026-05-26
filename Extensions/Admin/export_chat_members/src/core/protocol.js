// Единый словарь сообщений и общих констант для всех слоёв.

export const BRIDGE_TAG = 'yandex-chat-members-exporter';

export const MSG = Object.freeze({
  // page -> content -> background
  MEMBERS_BATCH: 'members-batch',
  PAGINATION_DONE: 'pagination-done',
  PAGINATION_ERROR: 'pagination-error',
  INTERCEPTOR_READY: 'interceptor-ready',
  LOG: 'log',

  // background -> content -> page
  START_PAGINATION: 'start-pagination',
  RESET_PAGE_STATE: 'reset-page-state',

  // popup <-> background
  GET_STATE: 'get-state',
  RESET_SESSION: 'reset-session',
  START_EXPORT: 'start-export',
  STATE_UPDATED: 'state-updated'
});

export const TARGET_ENDPOINT = '/api/models?_models=messenger/get-chat-members';
export const MODEL_NAME = 'messenger/get-chat-members';
