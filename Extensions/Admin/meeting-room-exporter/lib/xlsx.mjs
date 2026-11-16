/**
 * Browser-only, dependency-free SpreadsheetML export in a stored ZIP package.
 * OOXML package/worksheet structure and cell types follow Microsoft's docs:
 * https://learn.microsoft.com/en-us/office/open-xml/spreadsheet/structure-of-a-spreadsheetml-document
 * https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.spreadsheet.cell
 * https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.spreadsheet.cellvalues
 * Excel limits: https://support.microsoft.com/en-us/excel/excel-specifications-and-limits
 * ZIP stored records/CRC32: https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
 */
const XML = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>';
const SHEET_NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main';
const DOCUMENT_REL = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships';
const PACKAGE_REL = 'http://schemas.openxmlformats.org/package/2006/relationships';
const MAX_ROWS = 1048576;
const MAX_COLUMNS = 16384;
const MAX_TEXT = 32767;
const encoder = new TextEncoder();

const WIDTHS = {
  'Название': 34, 'Город': 24, 'Расположение': 44, 'Вместимость': 18,
  'Почта': 44, 'Описание': 64, 'Активирована в Телемосте': 28,
  'Доступна для бронирования': 28, 'Офис': 32, 'Адрес офиса': 50,
  'Этаж': 12, 'ID комнаты': 26, 'ID офиса': 26, 'Тип ресурса': 26,
};

function columnName(index) {
  let name = '';
  for (let value = index + 1; value; value = Math.floor((value - 1) / 26)) {
    name = String.fromCharCode(65 + (value - 1) % 26) + name;
  }
  return name;
}

function cleanText(value, address) {
  const valid = [];
  for (const char of value) {
    const cp = char.codePointAt(0);
    if (cp === 9 || cp === 10 || cp === 13 ||
        (cp >= 0x20 && cp <= 0xD7FF) || (cp >= 0xE000 && cp <= 0xFFFD) ||
        (cp >= 0x10000 && cp <= 0x10FFFF)) valid.push(char);
  }
  const text = valid.join('');
  if (text.length > MAX_TEXT) throw new Error(`Ячейка ${address}: Excel допускает не более 32 767 символов.`);
  if ((text.match(/\n/gu) || []).length > 253) throw new Error(`Ячейка ${address}: Excel допускает не более 253 переносов строки.`);
  return text;
}

function xmlText(text) {
  // Escape literal OOXML escape sequences before XML escaping. Without this,
  // Excel could interpret a literal "_x000A_" as a line-feed character.
  return text.replace(/_x([0-9a-fA-F]{4})_/gu, '_x005F_x$1_')
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&apos;').replaceAll('\r', '&#13;');
}

function isIdentifier(column) {
  return /^ID(?:\s|$)/iu.test(column) || /^API: (?:.*\.)?id$/iu.test(column);
}

function cell(value, column, address, header = false) {
  const identifier = !header && isIdentifier(column);
  const style = header ? 1 : identifier ? 3 : 2;
  if (typeof value === 'number' && !Number.isFinite(value)) {
    throw new Error(`Ячейка ${address}: невозможно сохранить бесконечность или NaN в Excel.`);
  }
  if (!header && !identifier && typeof value === 'number') {
    return { xml: `<c r="${address}" s="${style}" t="n"><v>${value}</v></c>`, text: String(value) };
  }
  if (!header && !identifier && typeof value === 'boolean') {
    return { xml: `<c r="${address}" s="${style}" t="b"><v>${value ? 1 : 0}</v></c>`, text: String(value) };
  }
  let valueText;
  if (value === null || value === undefined) valueText = '';
  else if (typeof value === 'object') {
    try { valueText = JSON.stringify(value); }
    catch { throw new Error(`Ячейка ${address}: значение невозможно преобразовать в текст.`); }
  } else valueText = String(value);
  const text = cleanText(valueText ?? '', address);
  // Inline strings remain literal text, even when their first character is '='.
  return { xml: `<c r="${address}" s="${style}" t="inlineStr"><is><t xml:space="preserve">${xmlText(text)}</t></is></c>`, text };
}

function worksheet(rows, columns) {
  const widths = columns.map(column => WIDTHS[column] || Math.min(44, Math.max(20, column.length + 3)));
  const names = columns.map((_, index) => columnName(index));
  const lastCell = `${names.at(-1)}${rows.length + 1}`;
  const sheetRows = [`<row r="1" ht="36" customHeight="1">${columns.map((column, index) => cell(column, column, `${names[index]}1`, true).xml).join('')}</row>`];
  for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
    const row = rows[rowIndex];
    if (row === null || typeof row !== 'object' || Array.isArray(row)) throw new TypeError(`Некорректная строка ${rowIndex + 1} для XLSX.`);
    const cells = columns.map((column, index) => cell(row[column], column, `${names[index]}${rowIndex + 2}`));
    const lines = Math.max(1, ...cells.map((item, index) => item.text.split(/\r\n|\r|\n/u)
      .reduce((count, line) => count + Math.max(1, Math.ceil([...line].length / Math.max(8, widths[index] - 2))), 0)));
    const height = Math.min(409, Math.max(30, lines * 15));
    sheetRows.push(`<row r="${rowIndex + 2}" ht="${height}" customHeight="1">${cells.map(item => item.xml).join('')}</row>`);
  }
  return `${XML}<worksheet xmlns="${SHEET_NS}"><dimension ref="A1:${lastCell}"/>` +
    '<sheetViews><sheetView workbookViewId="0" showGridLines="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/><selection pane="bottomLeft" activeCell="A2" sqref="A2"/></sheetView></sheetViews>' +
    '<sheetFormatPr defaultRowHeight="15"/><cols>' +
    widths.map((width, index) => `<col min="${index + 1}" max="${index + 1}" width="${width}" customWidth="1"/>`).join('') +
    `</cols><sheetData>${sheetRows.join('')}</sheetData><autoFilter ref="A1:${lastCell}"/></worksheet>`;
}

const styles = `${XML}<styleSheet xmlns="${SHEET_NS}">` +
  '<fonts count="2"><font><sz val="11"/><name val="Arial"/><family val="2"/></font><font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Arial"/><family val="2"/></font></fonts>' +
  '<fills count="3"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF214C71"/><bgColor indexed="64"/></patternFill></fill></fills>' +
  '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>' +
  '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>' +
  '<cellXfs count="4"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>' +
  '<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>' +
  '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>' +
  '<xf numFmtId="49" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf></cellXfs>' +
  '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>';

const crcTable = Uint32Array.from({ length: 256 }, (_, index) => {
  let value = index;
  for (let bit = 0; bit < 8; bit++) value = (value & 1) ? 0xEDB88320 ^ (value >>> 1) : value >>> 1;
  return value >>> 0;
});

function crc32(bytes) {
  let crc = 0xFFFFFFFF;
  for (const byte of bytes) crc = crcTable[(crc ^ byte) & 0xFF] ^ (crc >>> 8);
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

function storedZip(files) {
  const parts = files.map(([path, text]) => ({ name: encoder.encode(path), data: encoder.encode(text) }));
  let offset = 0;
  let centralSize = 0;
  for (const part of parts) {
    part.offset = offset;
    part.crc = crc32(part.data);
    offset += 30 + part.name.length + part.data.length;
    centralSize += 46 + part.name.length;
  }
  const totalSize = offset + centralSize + 22;
  if (totalSize > 0xFFFFFFFF) throw new Error('Файл слишком большой для встроенного экспорта XLSX (предел ZIP — 4 ГБ).');
  const output = new Uint8Array(totalSize);
  const view = new DataView(output.buffer);
  for (const part of parts) {
    const at = part.offset;
    view.setUint32(at, 0x04034B50, true);
    view.setUint16(at + 4, 20, true);
    view.setUint16(at + 6, 0x0800, true);
    view.setUint16(at + 12, 0x0021, true); // 1980-01-01, no personal metadata.
    view.setUint32(at + 14, part.crc, true);
    view.setUint32(at + 18, part.data.length, true);
    view.setUint32(at + 22, part.data.length, true);
    view.setUint16(at + 26, part.name.length, true);
    output.set(part.name, at + 30);
    output.set(part.data, at + 30 + part.name.length);
  }
  const centralOffset = offset;
  for (const part of parts) {
    view.setUint32(offset, 0x02014B50, true);
    view.setUint16(offset + 4, 20, true);
    view.setUint16(offset + 6, 20, true);
    view.setUint16(offset + 8, 0x0800, true);
    view.setUint16(offset + 14, 0x0021, true);
    view.setUint32(offset + 16, part.crc, true);
    view.setUint32(offset + 20, part.data.length, true);
    view.setUint32(offset + 24, part.data.length, true);
    view.setUint16(offset + 28, part.name.length, true);
    view.setUint32(offset + 42, part.offset, true);
    output.set(part.name, offset + 46);
    offset += 46 + part.name.length;
  }
  view.setUint32(offset, 0x06054B50, true);
  view.setUint16(offset + 8, parts.length, true);
  view.setUint16(offset + 10, parts.length, true);
  view.setUint32(offset + 12, centralSize, true);
  view.setUint32(offset + 16, centralOffset, true);
  return output;
}

/** Export every supplied row and column as a genuine, formula-free XLSX file. */
export function toXlsx(rows, columns) {
  if (!Array.isArray(rows) || !Array.isArray(columns) || columns.some(column => typeof column !== 'string')) {
    throw new TypeError('Для XLSX нужны строки и список названий столбцов.');
  }
  if (columns.length < 1 || columns.length > MAX_COLUMNS) throw new Error('Excel допускает от 1 до 16 384 столбцов на листе.');
  if (rows.length + 1 > MAX_ROWS) throw new Error('Excel допускает не более 1 048 576 строк, включая заголовок.');
  return storedZip([
    ['[Content_Types].xml', `${XML}<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>`],
    ['_rels/.rels', `${XML}<Relationships xmlns="${PACKAGE_REL}"><Relationship Id="rId1" Type="${DOCUMENT_REL}/officeDocument" Target="xl/workbook.xml"/></Relationships>`],
    ['xl/workbook.xml', `${XML}<workbook xmlns="${SHEET_NS}" xmlns:r="${DOCUMENT_REL}"><bookViews><workbookView/></bookViews><sheets><sheet name="Переговорные" sheetId="1" r:id="rId1"/></sheets></workbook>`],
    ['xl/_rels/workbook.xml.rels', `${XML}<Relationships xmlns="${PACKAGE_REL}"><Relationship Id="rId1" Type="${DOCUMENT_REL}/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="${DOCUMENT_REL}/styles" Target="styles.xml"/></Relationships>`],
    ['xl/styles.xml', styles],
    ['xl/worksheets/sheet1.xml', worksheet(rows, columns)],
  ]);
}
