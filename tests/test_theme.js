const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { runInNewContext } = require('node:vm');
const source = readFileSync(join(__dirname, '../apps/static/assets/js/theme.js'), 'utf8');

function browser({ saved = null, dark = false, blocked = false } = {}) {
  const documentEvents = {}, windowEvents = {}, controlEvents = {};
  const root = { dataset: {} };
  const select = { value: '', addEventListener: (event, fn) => { controlEvents[event] = fn; } };
  const system = { matches: dark, addEventListener: (_, fn) => { system.change = fn; } };
  const storage = new Map(saved ? [['pgassistant.theme', saved]] : []);
  const document = {
    documentElement: root,
    querySelectorAll: selector => ready ? (selector === '[data-pga-theme-select]' ? [select] : [toggle]) : [],
    addEventListener: (event, fn) => { documentEvents[event] = fn; }
  };
  const toggleEvents = {}, attributes = {}, icon = {};
  const toggle = { setAttribute: (key, value) => { attributes[key] = value; }, querySelector: () => icon,
    addEventListener: (event, fn) => { toggleEvents[event] = fn; } };
  let ready = false;
  const window = {
    matchMedia: () => system,
    localStorage: {
      getItem: key => { if (blocked) throw new Error('Storage unavailable'); return storage.get(key); },
      setItem: (key, value) => { if (blocked) throw new Error('Storage unavailable'); storage.set(key, value); }
    },
    addEventListener: (event, fn) => { windowEvents[event] = fn; }
  };
  runInNewContext(source, { window, document });
  const firstPaintTheme = root.dataset.bsTheme;
  ready = true;
  documentEvents.DOMContentLoaded();
  return { root, select, system, storage, firstPaintTheme, attributes, icon, toggle() { toggleEvents.click(); },
    choose(value) { select.value = value; controlEvents.change(); },
    storageEvent(value) { windowEvents.storage({ key: 'pgassistant.theme', newValue: value }); }
  };
}

test('saved light overrides a dark system before the first paint', () => {
  const page = browser({ saved: 'light', dark: true });
  assert.equal(page.firstPaintTheme, 'light');
  assert.equal(page.select.value, 'light');
});
test('a selected theme persists across page loads and ignores system changes', () => {
  const page = browser();
  page.choose('dark');
  assert.equal(page.root.dataset.bsTheme, 'dark');
  assert.equal(browser({ saved: page.storage.get('pgassistant.theme') }).firstPaintTheme, 'dark');
  page.system.matches = false;
  page.system.change();
  assert.equal(page.root.dataset.bsTheme, 'dark');
});
test('system mode follows operating system changes', () => {
  const page = browser({ saved: 'light' });
  page.choose('system');
  page.system.matches = true;
  page.system.change();
  assert.equal(page.root.dataset.bsTheme, 'dark');
  assert.equal(page.select.value, 'system');
});
test('blocked storage does not prevent changing appearance', () => {
  const page = browser({ blocked: true, dark: true });
  assert.equal(page.firstPaintTheme, 'dark');
  page.choose('light');
  assert.equal(page.root.dataset.bsTheme, 'light');
});
test('theme preference synchronizes across tabs and resets safely', () => {
  const page = browser();
  page.storageEvent('dark');
  assert.equal(page.root.dataset.bsTheme, 'dark');
  assert.equal(page.select.value, 'dark');
  page.storageEvent(null);
  assert.equal(page.root.dataset.pgaTheme, 'system');
  assert.equal(page.root.dataset.bsTheme, 'light');
});

test('icon button toggles appearance and updates its accessible label', () => {
  const page = browser({ saved: 'light' });
  page.toggle();
  assert.equal(page.root.dataset.bsTheme, 'dark');
  assert.equal(page.attributes['aria-label'], 'Switch to light mode');
  assert.equal(page.attributes['aria-pressed'], 'true');
  assert.equal(page.icon.className, 'bi bi-sun');
  assert.equal(page.storage.get('pgassistant.theme'), 'dark');
  page.toggle();
  assert.equal(page.root.dataset.bsTheme, 'light');
  assert.equal(page.attributes['aria-label'], 'Switch to dark mode');
});
