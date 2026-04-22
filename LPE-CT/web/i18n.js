(function initLpeCtI18n(global) {
  const DEFAULT_LOCALE = "en";

  function isObject(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }

  function deepMerge(base, override) {
    if (!isObject(base) || !isObject(override)) {
      return override ?? base;
    }

    const merged = { ...base };
    Object.entries(override).forEach(([key, value]) => {
      const baseValue = base[key];
      if (isObject(baseValue) && isObject(value)) {
        merged[key] = deepMerge(baseValue, value);
        return;
      }
      merged[key] = value;
    });
    return merged;
  }

  function deepFreeze(value) {
    if (!isObject(value) && !Array.isArray(value)) {
      return value;
    }

    Object.values(value).forEach((entry) => {
      if ((isObject(entry) || Array.isArray(entry)) && !Object.isFrozen(entry)) {
        deepFreeze(entry);
      }
    });

    return Object.freeze(value);
  }

  function defineLocaleCatalog({ supportedLocales, defaultLocale = DEFAULT_LOCALE, base, localized = {} }) {
    const catalog = {};

    supportedLocales.forEach((locale) => {
      if (locale === defaultLocale) {
        catalog[locale] = deepFreeze(deepMerge(base, {}));
        return;
      }
      catalog[locale] = deepFreeze(deepMerge(base, localized[locale] ?? {}));
    });

    return deepFreeze(catalog);
  }

  function resolveBrowserLocale(supportedLocales) {
    const browserLocales = global.navigator?.languages ?? [global.navigator?.language].filter(Boolean);
    for (const locale of browserLocales) {
      if (typeof locale !== "string" || !locale.trim()) {
        continue;
      }
      const normalized = locale.toLowerCase();
      if (supportedLocales.includes(normalized)) {
        return normalized;
      }
      const language = normalized.split("-")[0];
      if (supportedLocales.includes(language)) {
        return language;
      }
    }
    return DEFAULT_LOCALE;
  }

  function createI18n({
    storageKey,
    supportedLocales,
    localeLabels,
    messages,
    defaultLocale = DEFAULT_LOCALE,
    root = global.document,
  }) {
    let currentLocale = defaultLocale;
    let boundPickers = [];

    try {
      const storedLocale = global.localStorage.getItem(storageKey);
      if (supportedLocales.includes(storedLocale)) {
        currentLocale = storedLocale;
      } else {
        currentLocale = resolveBrowserLocale(supportedLocales);
      }
    } catch {
      currentLocale = resolveBrowserLocale(supportedLocales);
    }

    function getCopy() {
      return messages[currentLocale] ?? messages[defaultLocale];
    }

    function getMessage(key) {
      const copy = getCopy();
      if (Object.prototype.hasOwnProperty.call(copy, key)) {
        return copy[key];
      }
      return messages[defaultLocale]?.[key] ?? key;
    }

    function syncLocalePickers() {
      boundPickers.forEach((picker) => {
        picker.value = currentLocale;
      });
    }

    function bindLocalePickers(pickers, onChange) {
      boundPickers = Array.from(pickers);
      boundPickers.forEach((picker) => {
        picker.innerHTML = "";
        supportedLocales.forEach((locale) => {
          const option = document.createElement("option");
          option.value = locale;
          option.textContent = localeLabels[locale] ?? locale;
          picker.appendChild(option);
        });
        picker.addEventListener("change", (event) => {
          onChange(event.target.value);
        });
      });
      syncLocalePickers();
    }

    function translate(template, values = {}) {
      return String(template ?? "").replace(/\{(\w+)\}/g, (_, key) => String(values[key] ?? ""));
    }

    function applyTranslations(targetRoot = root) {
      targetRoot.querySelectorAll("[data-i18n]").forEach((element) => {
        const key = element.dataset.i18n;
        if (key) {
          element.textContent = getMessage(key);
        }
      });

      targetRoot.querySelectorAll("[data-i18n-placeholder]").forEach((element) => {
        const key = element.dataset.i18nPlaceholder;
        if (key) {
          element.setAttribute("placeholder", getMessage(key));
        }
      });

      targetRoot.querySelectorAll("[data-i18n-aria-label]").forEach((element) => {
        const key = element.dataset.i18nAriaLabel;
        if (key) {
          element.setAttribute("aria-label", getMessage(key));
        }
      });

      targetRoot.querySelectorAll("[data-i18n-title]").forEach((element) => {
        const key = element.dataset.i18nTitle;
        if (key) {
          element.setAttribute("title", getMessage(key));
        }
      });
    }

    function setLocale(locale) {
      currentLocale = supportedLocales.includes(locale) ? locale : defaultLocale;
      try {
        global.localStorage.setItem(storageKey, currentLocale);
      } catch {}
      global.document.documentElement.lang = currentLocale;
      const pageTitle = getMessage("pageTitle");
      if (pageTitle) {
        global.document.title = pageTitle;
      }
      syncLocalePickers();
      applyTranslations(root);
      return currentLocale;
    }

    return Object.freeze({
      applyTranslations,
      bindLocalePickers,
      getCopy,
      getLocale: () => currentLocale,
      getMessage,
      setLocale,
      supportedLocales,
      translate,
    });
  }

  global.LpeCtI18n = Object.freeze({
    createI18n,
    defineLocaleCatalog,
  });
})(window);
