/* global document, ResizeObserver, MutationObserver, matchMedia, getComputedStyle */
(() => {
  // Zensical owns fetching and caching repository facts; add names to its icon-only values.
  const repository = document.querySelector(".sk-repository");
  if (repository) {
    const name = repository.getAttribute("aria-label");
    const labels = { version: "Latest release", stars: "Stars", forks: "Forks" };
    const describeRepository = () => {
      const descriptions = [];
      for (const [key, label] of Object.entries(labels)) {
        const fact = repository.querySelector(`.md-source__fact--${key}`);
        if (!fact) continue;
        const description = `${label}: ${fact.textContent.trim()}`;
        fact.setAttribute("aria-label", description);
        fact.setAttribute("title", description);
        descriptions.push(description);
      }
      repository.setAttribute("aria-label", [name, ...descriptions].join(". "));
    };
    describeRepository();
    new MutationObserver(describeRepository).observe(repository, {
      childList: true,
      subtree: true,
    });
  }
  const trigger = document.querySelector(".sk-nav-trigger");
  const sidebar = document.querySelector(".md-sidebar--primary");
  const drawer = document.querySelector("#__drawer");
  if (trigger && sidebar && drawer) {
    // Keep Zensical's single navigation tree and checkbox-driven drawer styling.
    const mobile = matchMedia("(max-width: 76.234375em)");
    sidebar.id = "documentation-navigation";
    const activeLinks = [...sidebar.querySelectorAll("a.md-nav__link--active")];
    const current =
      activeLinks.find((link) => !link.closest(".md-nav__container")) || activeLinks[0];
    current?.setAttribute("aria-current", "page");
    // Section indexes open their overview; the adjacent arrow can expand
    // the section without navigating. Give that icon-only control a name.
    for (const container of sidebar.querySelectorAll(".md-nav__container")) {
      const link = container.querySelector("a.md-nav__link");
      const toggle = container.querySelector("label.md-nav__link[for]");
      if (!link || !toggle) continue;
      const name = link.textContent.trim();
      const state = document.getElementById(toggle.htmlFor);
      const section = container.parentElement.querySelector("nav");
      toggle.setAttribute("aria-label", name);
      toggle.setAttribute("role", "button");
      if (section) {
        section.id ||= `${toggle.htmlFor}_section`;
        toggle.setAttribute("aria-controls", section.id);
      }
      toggle.addEventListener("keydown", (event) => {
        if (event.key === " ") {
          event.preventDefault();
          state.click();
        }
      });
      function syncSection() {
        toggle.setAttribute("aria-expanded", String(state.checked));
        section?.setAttribute("aria-expanded", String(state.checked));
      }
      state.addEventListener("change", syncSection);
      syncSection();
    }
    const close = document.createElement("button");
    close.type = "button";
    close.className = "sk-nav-close";
    close.textContent = "Close navigation";
    sidebar.querySelector(".md-sidebar__inner").prepend(close);
    // The theme's named label is not a valid accessible close control.
    const backdrop = document.createElement("button");
    backdrop.type = "button";
    backdrop.className = "md-overlay";
    backdrop.tabIndex = -1;
    backdrop.setAttribute("aria-label", "Close navigation");
    backdrop.setAttribute("aria-controls", sidebar.id);
    document.querySelector(".md-overlay")?.replaceWith(backdrop);
    let wasOpen = false;
    function syncDrawer() {
      const open = mobile.matches && drawer.checked;
      sidebar.inert = mobile.matches && !open;
      backdrop.hidden = !open;
      backdrop.disabled = !open;
      trigger.setAttribute("aria-expanded", String(open));
      if (open && !wasOpen) close.focus();
      if (!open && wasOpen && mobile.matches) trigger.focus();
      wasOpen = open;
    }
    trigger.addEventListener("click", () => drawer.click());
    close.addEventListener("click", () => drawer.click());
    backdrop.addEventListener("click", () => drawer.click());
    drawer.addEventListener("change", syncDrawer);
    mobile.addEventListener("change", () => {
      const focusWasInSidebar = wasOpen || sidebar.contains(document.activeElement);
      if (drawer.checked) drawer.click();
      syncDrawer();
      if (focusWasInSidebar) {
        const target = mobile.matches
          ? trigger
          : current ||
            sidebar.querySelector(".md-nav__item--active .md-nav__container a") ||
            sidebar.querySelector(".md-nav__list a");
        target?.focus();
      }
    });
    document.addEventListener(
      "keydown",
      (event) => {
        if (!mobile.matches || !drawer.checked) return;
        // Release drawer focus before the theme opens search for its shortcut.
        if (
          (event.key.toLowerCase() === "k" && (event.ctrlKey || event.metaKey)) ||
          event.key === "/"
        ) {
          drawer.click();
        } else if (event.key === "Escape") {
          event.preventDefault();
          drawer.click();
        } else if (event.key === "Tab") {
          const items = [...sidebar.querySelectorAll("a[href], button, [tabindex='0']")].filter(
            (element) =>
              element.getClientRects().length && getComputedStyle(element).visibility !== "hidden",
          );
          const first = items[0];
          const last = items[items.length - 1];
          if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last.focus();
          } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first.focus();
          }
        }
      },
      { capture: true },
    );
    syncDrawer();
  }
  document.querySelector(".sk-search-trigger")?.addEventListener("click", () => {
    document.querySelector(".md-search__button")?.click();
  });
  // Zensical wraps wide tables after parsing. Make actual scroll regions reachable
  // by keyboard, without adding unnecessary Tab stops to tables that already fit.
  new ResizeObserver(() => {
    for (const region of document.querySelectorAll(".md-typeset__scrollwrap")) {
      if (region.scrollWidth > region.clientWidth) {
        region.tabIndex = 0;
        region.setAttribute("role", "region");
        const columns = [...region.querySelectorAll("thead th")]
          .map((cell) => cell.textContent.trim())
          .join(", ");
        region.setAttribute("aria-label", `Scrollable table: ${columns}`);
      } else {
        for (const name of ["tabindex", "role", "aria-label"]) region.removeAttribute(name);
      }
    }
  }).observe(document.body);
})();
