/* Apply the saved appearance before styles load, then wire the shared picker. */
(function () {
  'use strict';

  const key = 'pgassistant.theme';
  const root = document.documentElement;
  const system = window.matchMedia('(prefers-color-scheme: dark)');
  const validMode = value => ['light', 'dark', 'system'].includes(value) ? value : 'system';
  let mode = 'system';
  try {
    mode = validMode(window.localStorage.getItem(key));
  } catch (_) {
    // Appearance still works when browser storage is unavailable.
  }

  function apply() {
    root.dataset.bsTheme = mode === 'system' ? (system.matches ? 'dark' : 'light') : mode;
    root.dataset.pgaTheme = mode;
    document.querySelectorAll('[data-pga-theme-toggle]').forEach(button => {
      const dark = root.dataset.bsTheme === 'dark';
      const label = dark ? 'Switch to light mode' : 'Switch to dark mode';
      button.setAttribute('aria-label', label);
      button.setAttribute('title', label);
      button.setAttribute('aria-pressed', String(dark));
      const icon = button.querySelector('[data-pga-theme-icon]');
      if (icon) icon.className = dark ? 'bi bi-sun' : 'bi bi-moon';
    });
    document.querySelectorAll('[data-pga-theme-select]').forEach(select => {
      select.value = mode;
    });
  }

  apply();
  document.addEventListener('DOMContentLoaded', function () {
    apply();
    document.querySelectorAll('[data-pga-theme-toggle]').forEach(button => {
      button.addEventListener('click', function () {
        mode = root.dataset.bsTheme === 'dark' ? 'light' : 'dark';
        try { window.localStorage.setItem(key, mode); } catch (_) {}
        apply();
      });
    });
    document.querySelectorAll('[data-pga-theme-select]').forEach(select => {
      select.addEventListener('change', function () {
        mode = validMode(select.value);
        try {
          window.localStorage.setItem(key, mode);
        } catch (_) {
          // Keep the choice for this page even if it cannot be persisted.
        }
        apply();
      });
    });
  });
  system.addEventListener('change', function () {
    if (mode === 'system') apply();
  });
  window.addEventListener('storage', function (event) {
    if (event.key === key || event.key === null) {
      mode = validMode(event.newValue);
      apply();
    }
  });
})();
