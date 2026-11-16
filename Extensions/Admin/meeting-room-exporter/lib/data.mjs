/** Pure catalog conversion. No session credentials are consumed or exported. */
export const BASE_COLUMNS = Object.freeze([
  'Название',
  'Город',
  'Расположение',
  'Вместимость',
  'Почта',
  'Описание',
  'Активирована в Телемосте',
  'Доступна для бронирования',
  'Офис',
  'Адрес офиса',
  'Этаж',
  'ID комнаты',
  'ID офиса',
  'Тип ресурса',
]);

const KNOWN_PATHS = new Set([
  'name', 'capacity', 'email', 'description', 'floor', 'id',
  'can_book', 'resource_subtype', 'is_bound', 'office',
  'office.name', 'office.id', 'office.city', 'office.address',
]);

const isRecord = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const present = value => value !== undefined && value !== null && value !== '';

function display(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return value;
}

function displayCity(value) {
  if (!isRecord(value)) return display(value);
  for (const key of ['name', 'title', 'ru', 'en', 'value']) {
    if (present(value[key])) return displayCity(value[key]);
  }
  return display(value);
}

function displayFlag(value) {
  if (value === true) return 'Да';
  if (value === false) return 'Нет';
  return 'Не указано';
}

// Escaping preserves the distinction between a literal dotted key and nesting.
function escapeSegment(value) {
  return value.replaceAll('\\', '\\\\').replaceAll('.', '\\.');
}

function flatten(value, path, output, ancestors = new Set()) {
  if (isRecord(value) && Object.keys(value).length) {
    if (ancestors.has(value)) throw new Error('Каталог содержит циклическую структуру.');
    ancestors.add(value);
    for (const [key, child] of Object.entries(value)) {
      flatten(child, path ? `${path}.${escapeSegment(key)}` : escapeSegment(key), output, ancestors);
    }
    ancestors.delete(value);
    return;
  }
  if (path && !KNOWN_PATHS.has(path)) output.set(`API: ${path}`, display(value));
}

/** Human-readable rows plus every additional API field in dynamic columns. */
export function normalizeRooms(rooms, offices = []) {
  if (!Array.isArray(rooms)) throw new TypeError('Список переговорных должен быть массивом.');
  if (!Array.isArray(offices)) throw new TypeError('Список офисов должен быть массивом.');
  const officeById = new Map();
  for (const office of offices) {
    if (!isRecord(office)) throw new TypeError('Некорректная запись в списке офисов.');
    if (present(office.id)) officeById.set(String(office.id), office);
  }
  const extraColumns = new Set();
  const rows = rooms.map(room => {
    if (!isRecord(room)) throw new TypeError('Некорректная запись в списке переговорных.');
    const embeddedOffice = isRecord(room.office) ? room.office : {};
    const officeId = embeddedOffice.id ?? room.office_id ?? (typeof room.office === 'string' || typeof room.office === 'number' ? room.office : undefined);
    const directoryOffice = present(officeId) ? officeById.get(String(officeId)) || {} : {};
    const office = { ...directoryOffice, ...embeddedOffice };
    // Empty embedded labels are common; enrich them from the office directory.
    for (const field of ['name', 'city']) {
      if (!present(office[field]) && present(directoryOffice[field])) office[field] = directoryOffice[field];
    }
    const officeName = display(office.name);
    const floor = display(room.floor);
    const locationParts = [];
    if (present(officeName)) locationParts.push(String(officeName));
    if (present(floor)) locationParts.push(`этаж ${floor}`);
    const row = {
      'Название': display(room.name),
      'Город': displayCity(office.city),
      'Расположение': locationParts.join(', '),
      'Вместимость': display(room.capacity),
      'Почта': display(room.email),
      'Описание': display(room.description),
      'Активирована в Телемосте': displayFlag(room.is_bound),
      'Доступна для бронирования': displayFlag(room.can_book),
      'Офис': officeName,
      'Адрес офиса': display(office.address),
      'Этаж': floor,
      'ID комнаты': display(room.id),
      'ID офиса': display(office.id ?? officeId),
      'Тип ресурса': display(room.resource_subtype),
    };
    const extras = new Map();
    flatten({ ...room, office }, '', extras);
    for (const [column, value] of extras) {
      extraColumns.add(column);
      row[column] = value;
    }
    return row;
  });
  const columns = [...BASE_COLUMNS, ...[...extraColumns].sort()];
  // Give consumers a consistent shape when a field only exists on some rooms.
  for (const row of rows) {
    for (const column of columns) {
      if (!Object.hasOwn(row, column)) row[column] = '';
    }
  }
  return { rows, columns };
}

function csvCell(value) {
  const raw = display(value);
  let text = String(raw);
  // Preserve negative numeric values; neutralize formulas coming from strings.
  if (typeof raw === 'string' && /^[\s\u0000-\u001f\u007f-\u009f]*[=+\-@]/u.test(text)) text = `'${text}`;
  return /[;"\r\n]/u.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

/** Excel-compatible CSV with a UTF-8 BOM, semicolons and CRLF line endings. */
export function toCsv(rows, columns) {
  if (!Array.isArray(rows) || !Array.isArray(columns) || columns.some(column => typeof column !== 'string')) {
    throw new TypeError('Для CSV нужны строки и список названий столбцов.');
  }
  const lines = [columns.map(csvCell).join(';')];
  for (const row of rows) {
    if (!isRecord(row)) throw new TypeError('Некорректная строка CSV.');
    lines.push(columns.map(column => csvCell(row[column])).join(';'));
  }
  return `\uFEFF${lines.join('\r\n')}\r\n`;
}

