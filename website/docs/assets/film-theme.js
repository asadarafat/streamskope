/* global window, document, MutationObserver */
(() => {
  window.subscribeFilmTheme = (apply) => {
    const system = window.matchMedia("(prefers-color-scheme: dark)");
    let owner;
    try {
      if (window.parent !== window) owner = window.parent.document.body;
    } catch {
      // Cross-origin embeds follow the system preference.
    }
    const update = () => {
      const scheme = owner?.getAttribute("data-md-color-scheme");
      const theme = (scheme ? scheme === "slate" : system.matches) ? "dark" : "light";
      document.documentElement.dataset.theme = theme;
      apply(theme);
    };
    const observer = new MutationObserver(update);
    if (owner)
      observer.observe(owner, {
        attributes: true,
        attributeFilter: ["data-md-color-scheme"],
      });
    system.addEventListener("change", update);
    update();
    return () => {
      observer.disconnect();
      system.removeEventListener("change", update);
    };
  };
})();
