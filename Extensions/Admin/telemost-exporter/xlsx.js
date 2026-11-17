/**
 * Small, dependency-free XLSX writer for a browser extension.
 *
 * writeXlsx([{ name, columns: [{ key, label, width?, type? }], rows }], { title? })
 * returns a Uint8Array containing an OOXML ZIP archive. The archive uses STORE
 * compression; no server, CDN, eval, formula, macro, or external link is used.
 *
 * Types: text (default), number, datetime. Identifiers should be text strings.
 * Datetimes accept Date, epoch milliseconds, a YYYY-MM-DD string, or an ISO
 * timestamp with Z / an explicit offset. Excel stores the corresponding UTC
 * date/time without a timezone; callers should label date columns accordingly.
 * null / undefined / empty datetime or number values create blank cells.
 *
 * Text is always literal, including values beginning with =, +, -, or @.
 * Unsupported XML controls and unpaired UTF-16 surrogates are removed. Strings
 * longer than Excel's 32,767 UTF-16 units are rejected, never silently cut.
 * Row / column / ZIP32 limits are rejected with an actionable error.
 */

const encoder = new TextEncoder();
const MAX_ROWS = 1048576;
const MAX_COLS = 16384;
const MAX_TEXT = 32767;
const UINT32_MAX = 0xffffffff;
const DAY_MS = 86400000;
const XML = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>';
const NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main';
const REL_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships';

const crcTable = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let crc = n;
    for (let k = 0; k < 8; k++) crc = crc & 1 ? 0xedb88320 ^ (crc >>> 1) : crc >>> 1;
    table[n] = crc >>> 0;
  }
  return table;
})();

function fail(message) {
  throw new Error(`XLSX: ${message}`);
}

function cleanXml(value) {
  // XML 1.0 accepts tabs/newlines/CR, valid BMP characters and surrogate pairs.
  return String(value).replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\ufffe\uffff]|[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]/g, '');
}

function xml(value) {
  return cleanXml(value).replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

function cellText(value, location) {
  let text;
  try {
    text = typeof value === 'object' ? JSON.stringify(value) : String(value);
  } catch {
    fail(`${location}: значение нельзя преобразовать в текст.`);
  }
  text = cleanXml(text ?? '');
  if (text.length > MAX_TEXT) {
    fail(`${location}: текст длиннее ${MAX_TEXT} символов. Разделите данные на несколько ячеек или строк.`);
  }
  // ST_Xstring interprets _xHHHH_ sequences. Escape literal sequences first;
  // then encode carriage returns to avoid XML newline normalization.
  return xml(text.replace(/_x[0-9a-f]{4}_/gi, match => `_x005F_${match.slice(1)}`)
    .replace(/\r/g, '_x000D_'));
}

function colName(index) {
  let name = '';
  for (let n = index + 1; n > 0; n = Math.floor((n - 1) / 26)) {
    name = String.fromCharCode(65 + (n - 1) % 26) + name;
  }
  return name;
}

function uniqueName(value, used, index) {
  let base = cleanXml(value ?? `Лист ${index + 1}`).replace(/[\\/?*\[\]:]/g, ' ')
    .trim().replace(/^'+|'+$/g, '').trim();
  if (!base) base = `Лист ${index + 1}`;
  // Do not split a supplementary Unicode character at Excel's name boundary.
  const truncate = (str, length) => str.slice(0, length).replace(/[\ud800-\udbff]$/, '');
  base = truncate(base, 31);
  let name = base;
  for (let suffix = 2; used.has(name.toLowerCase()); suffix++) {
    const tail = ` (${suffix})`;
    name = truncate(base, 31 - tail.length) + tail;
  }
  used.add(name.toLowerCase());
  return name;
}

function excelDate(value, location) {
  let time;
  if (value instanceof Date) {
    time = value.getTime();
  } else if (typeof value === 'number') {
    time = value;
  } else if (typeof value === 'string') {
    const match = /^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2}))?$/.exec(value);
    if (!match) fail(`${location}: дата должна быть ISO 8601 с часовым поясом, Date или временем Unix в миллисекундах.`);
    const [, y, m, d, h = '00', min = '00', sec = '00', fraction = '', zone = 'Z'] = match;
    const year = Number(y), month = Number(m), day = Number(d);
    const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
    const offset = zone === 'Z' ? null : /^([+-])(\d{2}):(\d{2})$/.exec(zone);
    if (year < 1900 || year > 9999 || month < 1 || month > 12 || day < 1 || day > daysInMonth || Number(h) > 23 ||
        Number(min) > 59 || Number(sec) > 59 || (offset && (Number(offset[2]) > 23 || Number(offset[3]) > 59))) {
      fail(`${location}: некорректная календарная дата.`);
    }
    time = Date.UTC(year, month - 1, day, Number(h), Number(min), Number(sec), Number(fraction.padEnd(3, '0').slice(0, 3)));
    if (offset) time -= (offset[1] === '+' ? 1 : -1) * (Number(offset[2]) * 60 + Number(offset[3])) * 60000;
  } else {
    fail(`${location}: неподдерживаемое значение даты.`);
  }
  if (!Number.isFinite(time) || time < Date.UTC(1900, 0, 1) || time >= Date.UTC(10000, 0, 1)) {
    fail(`${location}: дата вне поддерживаемого диапазона 1900–9999.`);
  }
  let serial = time / DAY_MS + 25569;
  // Excel's 1900 calendar includes the fictitious 29 February 1900.
  if (serial < 61) serial--;
  return serial;
}

function typedCell(value, column, ref, sheetName) {
  if (value === null || value === undefined) return '';
  const location = `лист «${sheetName}», ячейка ${ref}`;
  if (column.type === 'number') {
    if (value === '') return '';
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      fail(`${location}: ожидалось конечное число. Идентификаторы и неизвестные значения храните как текст.`);
    }
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      fail(`${location}: число больше безопасной точности JavaScript. Передайте идентификатор строкой.`);
    }
    return `<c r="${ref}" s="2"><v>${value}</v></c>`;
  }
  if (column.type === 'datetime') {
    if (value === '') return '';
    return `<c r="${ref}" s="3"><v>${excelDate(value, location)}</v></c>`;
  }
  return `<c r="${ref}" t="inlineStr" s="0"><is><t xml:space="preserve">${cellText(value, location)}</t></is></c>`;
}

function worksheet(sheet) {
  const { name, columns, rows } = sheet;
  const lastColumn = colName(columns.length - 1);
  const lastRow = rows.length + 1;
  const fragments = [XML, `<worksheet xmlns="${NS}">`,
    `<dimension ref="A1:${lastColumn}${lastRow}"/>`,
    '<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/><selection pane="bottomLeft" activeCell="A2" sqref="A2"/></sheetView></sheetViews>',
    '<sheetFormatPr defaultRowHeight="15"/><cols>'];
  columns.forEach((column, index) => {
    const width = column.width ?? (column.type === 'number' ? 16 : column.type === 'datetime' ? 24 : 28);
    if (typeof width !== 'number' || !Number.isFinite(width) || width < 1 || width > 255) {
      fail(`лист «${name}», столбец ${index + 1}: ширина должна быть числом от 1 до 255.`);
    }
    fragments.push(`<col min="${index + 1}" max="${index + 1}" width="${width}" customWidth="1"/>`);
  });
  fragments.push('</cols><sheetData><row r="1" ht="42" customHeight="1">');
  columns.forEach((column, index) => {
    const ref = `${colName(index)}1`;
    fragments.push(`<c r="${ref}" t="inlineStr" s="1"><is><t xml:space="preserve">${cellText(column.label ?? column.key, `лист «${name}», заголовок ${ref}`)}</t></is></c>`);
  });
  fragments.push('</row>');
  const columnNames = columns.map((_, index) => colName(index));
  rows.forEach((row, index) => {
    if (row === null || typeof row !== 'object' || Array.isArray(row)) {
      fail(`лист «${name}», строка ${index + 2}: ожидался объект со значениями столбцов.`);
    }
    fragments.push(`<row r="${index + 2}">`);
    columns.forEach((column, col) => fragments.push(typedCell(row[column.key], column, `${columnNames[col]}${index + 2}`, name)));
    fragments.push('</row>');
  });
  fragments.push(`</sheetData><autoFilter ref="A1:${lastColumn}${lastRow}"/>`,
    '<pageMargins left="0.3" right="0.3" top="0.5" bottom="0.5" header="0.2" footer="0.2"/></worksheet>');
  return fragments.join('');
}

const STYLES = `${XML}<styleSheet xmlns="${NS}">
<numFmts count="1"><numFmt numFmtId="164" formatCode="yyyy-mm-dd hh:mm:ss"/></numFmts>
<fonts count="2"><font><sz val="11"/><color rgb="FF202A35"/><name val="Calibri"/><family val="2"/></font><font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font></fonts>
<fills count="3"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF1E354F"/><bgColor indexed="64"/></patternFill></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="4"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf><xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf><xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf></cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
<dxfs count="0"/><tableStyles count="0" defaultTableStyle="TableStyleMedium2" defaultPivotStyle="PivotStyleLight16"/></styleSheet>`;

function crc32(bytes) {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) crc = crcTable[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

function zip(files) {
  if (files.length > 65535) fail('слишком много файлов для ZIP32. Разделите экспорт на несколько книг.');
  let localSize = 0;
  let directorySize = 0;
  const entries = files.map(({ name, content }) => {
    const path = encoder.encode(name);
    const data = encoder.encode(content);
    if (path.length > 65535 || data.length > UINT32_MAX) fail('превышен размер файла ZIP32. Сузьте период экспорта.');
    const entry = { path, data, offset: localSize, crc: crc32(data) };
    localSize += 30 + path.length + data.length;
    directorySize += 46 + path.length;
    if (localSize > UINT32_MAX || directorySize > UINT32_MAX) fail('книга превышает лимит ZIP32 4 ГБ. Сузьте период экспорта.');
    return entry;
  });
  const totalSize = localSize + directorySize + 22;
  if (totalSize > UINT32_MAX) fail('книга превышает лимит ZIP32 4 ГБ. Сузьте период экспорта.');
  let result;
  try { result = new Uint8Array(totalSize); }
  catch { fail('недостаточно памяти для книги. Сузьте период экспорта.'); }
  const view = new DataView(result.buffer);
  let pos = 0;
  const u16 = value => { view.setUint16(pos, value, true); pos += 2; };
  const u32 = value => { view.setUint32(pos, value, true); pos += 4; };
  const bytes = value => { result.set(value, pos); pos += value.length; };
  for (const entry of entries) {
    u32(0x04034b50); u16(20); u16(0x0800); u16(0); u16(0); u16(33);
    u32(entry.crc); u32(entry.data.length); u32(entry.data.length);
    u16(entry.path.length); u16(0); bytes(entry.path); bytes(entry.data);
  }
  for (const entry of entries) {
    u32(0x02014b50); u16(20); u16(20); u16(0x0800); u16(0); u16(0); u16(33);
    u32(entry.crc); u32(entry.data.length); u32(entry.data.length);
    u16(entry.path.length); u16(0); u16(0); u16(0); u16(0); u32(0); u32(entry.offset); bytes(entry.path);
  }
  u32(0x06054b50); u16(0); u16(0); u16(entries.length); u16(entries.length);
  u32(directorySize); u32(localSize); u16(0);
  return result;
}

/** Create an XLSX with typed cells, frozen headers and filters. */
export async function writeXlsx(sheets, { title = 'Телемост — аналитика встреч' } = {}) {
  if (!Array.isArray(sheets) || !sheets.length) fail('нужен хотя бы один лист.');
  if (sheets.length > 65528) fail('слишком много листов для ZIP32. Разделите экспорт на несколько книг.');
  const usedNames = new Set();
  const normalized = sheets.map((sheet, index) => {
    if (!sheet || !Array.isArray(sheet.columns) || !sheet.columns.length || !Array.isArray(sheet.rows)) {
      fail(`лист ${index + 1}: нужны непустой список columns и массив rows.`);
    }
    const name = uniqueName(sheet.name, usedNames, index);
    if (sheet.columns.length > MAX_COLS) fail(`лист «${name}»: больше ${MAX_COLS} столбцов. Разделите данные на несколько листов.`);
    if (sheet.rows.length > MAX_ROWS - 1) fail(`лист «${name}»: больше ${MAX_ROWS - 1} строк данных. Разделите данные на несколько листов.`);
    const columns = sheet.columns.map((column, col) => {
      if (!column || typeof column.key !== 'string' || !column.key) fail(`лист «${name}», столбец ${col + 1}: нужен строковый key.`);
      const type = column.type ?? 'text';
      if (!['text', 'number', 'datetime'].includes(type)) fail(`лист «${name}», столбец ${col + 1}: неизвестный тип «${type}».`);
      return { ...column, type };
    });
    return { name, columns, rows: sheet.rows };
  });
  const contentTypes = `${XML}<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>${normalized.map((_, index) => `<Override PartName="/xl/worksheets/sheet${index + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`).join('')}</Types>`;
  const workbook = `${XML}<workbook xmlns="${NS}" xmlns:r="${REL_NS}"><workbookPr date1904="0"/><bookViews><workbookView/></bookViews><sheets>${normalized.map((sheet, index) => `<sheet name="${xml(sheet.name)}" sheetId="${index + 1}" r:id="rId${index + 1}"/>`).join('')}</sheets></workbook>`;
  const workbookRels = `${XML}<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">${normalized.map((_, index) => `<Relationship Id="rId${index + 1}" Type="${REL_NS}/worksheet" Target="worksheets/sheet${index + 1}.xml"/>`).join('')}<Relationship Id="rId${normalized.length + 1}" Type="${REL_NS}/styles" Target="styles.xml"/></Relationships>`;
  const files = [
    { name: '[Content_Types].xml', content: contentTypes },
    { name: '_rels/.rels', content: `${XML}<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="${REL_NS}/officeDocument" Target="xl/workbook.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="${REL_NS}/extended-properties" Target="docProps/app.xml"/></Relationships>` },
    { name: 'docProps/core.xml', content: `${XML}<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>${xml(title)}</dc:title><dc:creator>Telemost Exporter</dc:creator></cp:coreProperties>` },
    { name: 'docProps/app.xml', content: `${XML}<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"><Application>Telemost Exporter</Application></Properties>` },
    { name: 'xl/workbook.xml', content: workbook },
    { name: 'xl/_rels/workbook.xml.rels', content: workbookRels },
    { name: 'xl/styles.xml', content: STYLES },
    ...normalized.map((sheet, index) => ({ name: `xl/worksheets/sheet${index + 1}.xml`, content: worksheet(sheet) })),
  ];
  return zip(files);
}
