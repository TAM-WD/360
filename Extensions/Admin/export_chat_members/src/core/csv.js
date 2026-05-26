/**
 * CSV-сериализатор, общий для popup и (потенциально) фоновых задач.
 * UTF-8 + BOM, разделитель ";", безопасное экранирование RFC 4180.
 */

const SEP = ';';
const EOL = '\r\n';
const BOM = '﻿';

const escapeCell = (value) => {
  const s = value == null ? '' : String(value);
  if (s.includes(SEP) || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
};

export const buildMembersCsv = (members) => {
  const header = ['uid', 'name', 'login'].map(escapeCell).join(SEP);
  const rows = (members || []).map((m) =>
    [m.uid, m.name, m.login].map(escapeCell).join(SEP)
  );
  return BOM + [header, ...rows].join(EOL) + EOL;
};
