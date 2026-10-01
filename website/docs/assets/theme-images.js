/* global document, MutationObserver, URL */
(() => {
  const assets = new URL(".", document.currentScript.src);
  const update = () => {
    const theme = document.body.dataset.mdColorScheme === "slate" ? "dark" : "light";
    for (const image of document.querySelectorAll("img[data-sk-light]")) {
      const source = new URL(image.getAttribute(`data-sk-${theme}`), assets).href;
      if (image.getAttribute("src") !== source) image.setAttribute("src", source);
    }
  };
  update();
  new MutationObserver(update).observe(document.body, {
    attributes: true,
    attributeFilter: ["data-md-color-scheme"],
  });
})();
