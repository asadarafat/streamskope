/* global document */
(() => {
  const directory = document.querySelector(".sk-directory");
  if (!directory) return;
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && directory.open) {
      directory.open = false;
      directory.querySelector("summary").focus();
    }
  });
  document.addEventListener("click", (event) => {
    if (directory.open && !directory.contains(event.target)) directory.open = false;
  });
})();
