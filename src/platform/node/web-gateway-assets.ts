import { lstat } from "node:fs/promises";
import { join, resolve, sep } from "node:path";

import { readBoundedFile } from "./bounded-file";

const TYPES: Readonly<Record<string, string>> = {
  css: "text/css; charset=utf-8",
  html: "text/html; charset=utf-8",
  ico: "image/x-icon",
  js: "text/javascript; charset=utf-8",
  json: "application/json; charset=utf-8",
  png: "image/png",
  svg: "image/svg+xml",
  webp: "image/webp",
  woff2: "font/woff2",
};

export interface WebGatewayAsset {
  readonly content: Uint8Array;
  readonly contentType: string;
}

/** Static assets are fixed build output, never user-supplied filesystem paths. */
export async function readWebGatewayAsset(
  rendererRoot: string,
  pathname: string,
): Promise<WebGatewayAsset | undefined> {
  if (
    !pathname.startsWith("/") ||
    pathname.includes("%") ||
    pathname.includes("\\") ||
    pathname.includes("\0") ||
    pathname.split("/").some((segment) => segment === "." || segment === "..")
  )
    return undefined;
  const root = resolve(rendererRoot);
  const relative = pathname === "/" ? "index.html" : pathname.slice(1);
  const segments = relative.split("/");
  if (segments.some((segment) => segment.length === 0)) return undefined;
  const path = resolve(root, relative);
  if (!path.startsWith(root + sep)) return undefined;
  try {
    const rootMetadata = await lstat(root);
    if (!rootMetadata.isDirectory() || rootMetadata.isSymbolicLink()) return undefined;
    let current = root;
    for (const segment of segments.slice(0, -1)) {
      current = join(current, segment);
      const metadata = await lstat(current);
      if (!metadata.isDirectory() || metadata.isSymbolicLink()) return undefined;
    }
    const extension = relative.split(".").at(-1) ?? "";
    const contentType = TYPES[extension];
    if (contentType === undefined) return undefined;
    return {
      content: await readBoundedFile(path, 32 * 1_048_576, { rejectSymlinks: true }),
      contentType,
    };
  } catch {
    return undefined;
  }
}

export function webGatewayLoginPage(mode: "create" | "unlock"): string {
  const creating = mode === "create";
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Unlock StreamSkope</title><style>body{font:16px system-ui;margin:0;background:#14171e;color:#f5f6fa;display:grid;min-height:100vh;place-items:center}main{width:min(28rem,calc(100vw - 3rem))}h1{font-size:2rem}label{display:block;margin:1rem 0 .4rem}input,button{font:inherit;box-sizing:border-box;width:100%;padding:.8rem;border-radius:.4rem;border:1px solid #5f6a7b}input{background:#222733;color:inherit}button{margin-top:1.2rem;background:#526ee0;color:white;cursor:pointer}p{line-height:1.5;color:#cad0de}[role=alert]{color:#ffb3b3;min-height:1.5rem}</style></head><body><main><h1>StreamSkope</h1><p>${creating ? "Create an encrypted vault for this instance. Retrieve the setup code from its private data directory before continuing." : "Unlock this instance to use your saved connections and plugins."}</p><form data-mode="${mode}">${creating ? '<label for="setup">Setup code</label><input id="setup" name="setupCode" type="password" autocomplete="off" required>' : ""}<label for="passphrase">Vault passphrase</label><input id="passphrase" name="passphrase" type="password" autocomplete="${creating ? "new-password" : "current-password"}" minlength="12" maxlength="1024" required>${creating ? '<label for="confirm">Confirm vault passphrase</label><input id="confirm" name="confirmation" type="password" autocomplete="new-password" minlength="12" maxlength="1024" required>' : ""}<button type="submit">${creating ? "Create vault" : "Unlock"}</button><p role="alert" aria-live="polite"></p></form></main><script src="/__streamskope_session/login.js"></script></body></html>`;
}

export const WEB_GATEWAY_LOGIN_SCRIPT = `const form=document.querySelector('form');form.addEventListener('submit',async event=>{event.preventDefault();const button=form.querySelector('button'),error=form.querySelector('[role=alert]');button.disabled=true;error.textContent='';try{const data=new FormData(form),body={passphrase:data.get('passphrase')};if(form.dataset.mode==='create'){if(body.passphrase!==data.get('confirmation'))throw new Error('The passphrases do not match.');body.setupCode=data.get('setupCode');}const response=await fetch('/__streamskope_session/'+form.dataset.mode,{method:'POST',credentials:'same-origin',headers:{'content-type':'application/json'},body:JSON.stringify(body)});if(!response.ok){const problem=await response.json();throw new Error(problem.error?.summary||'The vault could not be unlocked.')}form.reset();location.replace('/');}catch(problem){error.textContent=problem.message||'The vault could not be unlocked.';}finally{button.disabled=false;}});`;

export function prepareWebGatewayIndex(content: Uint8Array): Uint8Array {
  const html = new TextDecoder("utf-8", { fatal: true }).decode(content);
  return Buffer.from(
    html
      .replace(/connect-src [^;]*/u, "connect-src 'self'")
      .replace(
        "</head>",
        '<script src="/__streamskope_session/browser-runtime.js"></script></head>',
      ),
  );
}
