const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const settings = JSON.parse(fs.readFileSync(path.join(root, 'config.json'), 'utf8'));
const rules = JSON.parse(fs.readFileSync(path.join(root, 'rules.json'), 'utf8'));
module.exports = { settings, rules, base: { ...settings, rules: rules.rules } };
