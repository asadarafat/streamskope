import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { setTimeout as settle } from "node:timers/promises";
import { JSDOM, VirtualConsole } from "jsdom";

const adapter = await readFile("website/docs/assets/search-accessibility.js", "utf8");
async function fixture(t) {
  const virtualConsole = new VirtualConsole();
  virtualConsole.on("jsdomError", (error) => {
    throw error;
  });
  const dom = new JSDOM("<body><button>Open search</button></body>", {
    runScripts: "outside-only",
    virtualConsole,
  });
  t.after(() => dom.window.close());
  dom.window.eval(adapter);
  const host = dom.window.document.createElement("div");
  const root = host.attachShadow({ mode: "open" });
  root.innerHTML = `<div class="l d"><div class="k">
    <button><svg class="lucide-search"></svg></button>
    <div class="s"><input role="combobox"></div>
    <button><svg class="lucide-list-filter"></svg></button>
    </div><div class="z"><ol class="b"></ol></div>
    <div class="a d"><h3>Filters</h3></div></div>`;
  dom.window.document.body.append(host);
  await settle();
  return { dom, root, host, input: root.querySelector("input"), modal: root.querySelector(".l") };
}

test("names the real controls and hides closed modal/filter content", async (t) => {
  const { root, host, input, modal } = await fixture(t);
  assert.equal(host.getAttribute("data-sk-search"), "ready");
  assert.equal(input.getAttribute("aria-label"), "Search documentation");
  assert.equal(input.getAttribute("aria-expanded"), "false");
  assert.equal(modal.getAttribute("aria-hidden"), "true");
  assert(modal.hasAttribute("inert"));
  assert.equal(root.querySelector("button").getAttribute("aria-label"), "Close search");
  const filters = root.querySelector(".a");
  assert(filters.hasAttribute("inert"));
  filters.classList.remove("d");
  modal.classList.remove("d");
  await settle();
  assert.equal(filters.tabIndex, 0);
  assert.equal(filters.getAttribute("aria-hidden"), "false");
  assert(!filters.hasAttribute("inert"));
  assert.equal(root.querySelectorAll("button")[1].getAttribute("aria-expanded"), "true");
});

test("tracks dynamic result selection and clears stale active descendants", async (t) => {
  const { root, input, modal } = await fixture(t);
  const list = root.querySelector("ol.b");
  modal.classList.remove("d");
  list.innerHTML =
    '<li><a class="i h" href="/one">First</a></li><li><a class="i" href="/two">Second</a></li>';
  await settle();
  const [first, second] = list.querySelectorAll("a");
  assert.equal(input.getAttribute("aria-controls"), list.id);
  assert.equal(list.getAttribute("role"), "listbox");
  assert.equal(input.getAttribute("aria-expanded"), "true");
  assert.equal(input.getAttribute("aria-activedescendant"), first.id);
  assert.equal(first.getAttribute("role"), "option");
  first.classList.remove("h");
  second.classList.add("h");
  await settle();
  assert.equal(input.getAttribute("aria-activedescendant"), second.id);
  assert.equal(first.getAttribute("aria-selected"), "false");
  assert.equal(second.getAttribute("aria-selected"), "true");
  list.replaceChildren();
  await settle();
  assert.equal(input.getAttribute("aria-expanded"), "false");
  assert.equal(input.hasAttribute("aria-activedescendant"), false);
});

test("keeps filter/close Enter separate from upstream result navigation", async (t) => {
  const { dom, root, input } = await fixture(t);
  let calls = 0;
  dom.window.addEventListener("keydown", () => calls++);
  const key = () =>
    new dom.window.KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true });
  root.querySelector("button").dispatchEvent(key());
  assert.equal(calls, 0);
  input.dispatchEvent(key());
  assert.equal(calls, 1);
});

test("leaves unrelated shadow widgets untouched", async (t) => {
  const { dom } = await fixture(t);
  const host = dom.window.document.createElement("div");
  const root = host.attachShadow({ mode: "open" });
  root.innerHTML = '<input role="combobox"><button></button>';
  dom.window.document.body.append(host);
  await settle();
  assert.equal(host.hasAttribute("data-sk-search"), false);
  assert.equal(root.querySelector("button").hasAttribute("aria-label"), false);
});
