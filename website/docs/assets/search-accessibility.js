/* global document, MutationObserver */
((document) => {
  // Compatibility adapter for the pinned Zensical search widget. Keep its native
  // query, arrow-key selection and navigation; expose that behavior to AT.
  // Recheck this adapter against the real widget when upgrading Zensical.
  const observed = new WeakSet();
  const set = (element, name, value) => {
    if (element.getAttribute(name) !== value) element.setAttribute(name, value);
  };
  const remove = (element, name) => {
    if (element.hasAttribute(name)) element.removeAttribute(name);
  };
  function watch(host) {
    const root = host.shadowRoot;
    if (!root || observed.has(root)) return;
    observed.add(root);
    let input;
    let wasOpen = false;
    let returnFocus = document.activeElement;
    document.addEventListener("focusin", (event) => {
      if (!wasOpen && event.target !== host) returnFocus = event.target;
    });
    function update() {
      input = root.querySelector('.k .s input[role="combobox"]');
      const close = root.querySelector(".k .lucide-search")?.closest("button");
      const toggle = root.querySelector(".k .lucide-list-filter")?.closest("button");
      const modal = root.querySelector(".l");
      const list = root.querySelector("ol.b");
      const filters = root.querySelector(".a");
      if (!input || !close || !toggle || !modal || !list || !filters) return;
      set(host, "data-sk-search", "ready");
      if (!root.querySelector("style[data-sk-search]")) {
        const style = document.createElement("style");
        style.dataset.skSearch = "";
        style.textContent = `
          menu.n { color: var(--md-default-fg-color, #1c2025); }
          #sk-search-filters h4 { color: var(--md-default-fg-color--light, #5a626a); opacity: 1; }
          :focus-visible { outline: 2px solid var(--md-accent-fg-color, #244fc6); outline-offset: 2px; }
        `;
        root.append(style);
      }
      const open = !modal.classList.contains("d");
      const filtersOpen = !filters.classList.contains("d");
      const options = [...list.querySelectorAll("a.i")];
      set(list, "id", "sk-search-results");
      set(list, "role", "listbox");
      set(list, "aria-label", "Search results");
      set(list.parentElement, "role", "region");
      set(list.parentElement, "aria-label", "Search results area");
      set(list.parentElement, "tabindex", "0");
      options.forEach((option, index) => {
        set(option.parentElement, "role", "presentation");
        set(option, "id", `sk-search-result-${index}`);
        set(option, "role", "option");
        set(option, "aria-selected", String(option.classList.contains("h")));
        // The combobox owns keyboard selection; clicking an option still follows its href.
        set(option, "tabindex", "-1");
      });
      set(input, "aria-label", "Search documentation");
      set(input, "aria-autocomplete", "list");
      set(input, "aria-controls", list.id);
      set(input, "aria-expanded", String(open && options.length > 0));
      const selected = options.find((option) => option.classList.contains("h"));
      if (open && selected) set(input, "aria-activedescendant", selected.id);
      else remove(input, "aria-activedescendant");
      set(close, "aria-label", "Close search");
      set(toggle, "aria-label", "Search filters");
      set(toggle, "aria-expanded", String(open && filtersOpen));
      set(toggle, "aria-controls", "sk-search-filters");
      set(filters, "id", "sk-search-filters");
      set(filters, "role", "region");
      set(filters, "aria-label", "Search filters");
      set(filters, "aria-hidden", String(!filtersOpen));
      set(filters, "tabindex", filtersOpen ? "0" : "-1");
      if (filtersOpen) remove(filters, "inert");
      else set(filters, "inert", "");
      // Results scroll without moving focus out of the combobox. The filter pane
      // has its own keyboard stop when open, including when it has no tag buttons.
      set(modal, "role", "dialog");
      set(modal, "aria-label", "Search documentation");
      set(modal, "aria-modal", "true");
      set(modal, "aria-hidden", String(!open));
      if (open) {
        remove(modal, "inert");
        if (!wasOpen) {
          if (document.activeElement !== host) returnFocus = document.activeElement;
          input.focus();
        }
      } else {
        set(modal, "inert", "");
        if (wasOpen && returnFocus?.isConnected) returnFocus.focus();
      }
      wasOpen = open;
    }
    root.addEventListener("keydown", (event) => {
      if (event.key === "Tab" && wasOpen) {
        const controls = [...root.querySelectorAll("button, input, a[href], [tabindex]")].filter(
          (element) =>
            element.tabIndex >= 0 &&
            !element.disabled &&
            !element.closest("[inert]") &&
            element.getClientRects().length > 0,
        );
        const first = controls[0];
        const last = controls.at(-1);
        if (
          (event.shiftKey && root.activeElement === first) ||
          (!event.shiftKey && root.activeElement === last)
        ) {
          event.preventDefault();
          (event.shiftKey ? last : first)?.focus();
        }
        event.stopPropagation();
      }
      // Zensical handles result keys on window. Keep Enter on these buttons and
      // arrows in the filter pane from accidentally navigating a search result.
      if (input && event.target !== input && ["Enter", "ArrowUp", "ArrowDown"].includes(event.key))
        event.stopPropagation();
    });
    new MutationObserver(update).observe(root, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ["class"],
    });
    update();
  }
  const discover = () => document.querySelectorAll("body > div").forEach(watch);
  new MutationObserver(discover).observe(document.body, { childList: true });
  discover();
})(document);
