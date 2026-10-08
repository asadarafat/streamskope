// Local exact-source mitigations, not upstream releases. See the linked advisories.
// Retire only after unpatched upstream behavior passes the owned regressions; see
// dependency-maintenance.ts for the reviewed status and criteria. Original licenses remain intact.
export const BUILD_DEPENDENCY_PATCHES = [
  {
    name: "braces",
    version: "3.0.3",
    advisoryUrl: "https://github.com/advisories/GHSA-vfj7-8cjw-p6xm",
    registryUrl: "https://registry.npmjs.org/braces/-/braces-3.0.3.tgz",
    integrity:
      "sha512-yQbXgO/OSZVD2IsiLlro+7Hf6Q18EJrKSEsdoMzKePKXct3gvD8oLcOQdIzGupr5Fj+EDe8gO/lxc1BzfMpxvA==",
    files: [
      {
        file: "lib/parse.js",
        originalSha256: "e572166565f15fa6ad9865ae49d678218e32aabfd1b3720f6d0d43d39800d310",
        patchedSha256: "c074a2217b9694d53e074cd8dc2cf9c76ddf7fd6a7579055099e9b049e73ab66",
        replacements: [
          {
            before: "  const push = node => {",
            after:
              "  const push = node => {\n    if (stack.length > 128) throw new SyntaxError('Brace nesting exceeds the supported depth');",
          },
        ],
      },
      {
        file: "lib/compile.js",
        originalSha256: "dc98f22eee3d511785d92a00758d5f0d48efed5f5813bdecc2de430c529b5c9f",
        patchedSha256: "b44a33f244e460c37ef0429d01561fb1f883ded300ac90b4cda84b9dbabd5931",
        replacements: [
          {
            before: "  const walk = (node, parent = {}) => {",
            after:
              "  const walk = (node, parent = {}, depth = 0) => {\n    if (depth > 128) throw new SyntaxError('Brace nesting exceeds the supported depth');",
          },
          {
            before: "walk(child, node)",
            after: "walk(child, node, depth + 1)",
          },
        ],
      },
      {
        file: "lib/expand.js",
        originalSha256: "41ccc196ebfa7b7781a634e721eb744e4e7bcb54cba427a7e3d6806a1b9e58f7",
        patchedSha256: "6a8d95947e9884673b1b3dd98444e6ecf2e81734ee62f70eb5ad04abd7c92650",
        replacements: [
          {
            before: "  const walk = (node, parent = {}) => {",
            after:
              "  const walk = (node, parent = {}, depth = 0) => {\n    if (depth > 128) throw new SyntaxError('Brace nesting exceeds the supported depth');",
          },
          {
            before: "walk(child, node)",
            after: "walk(child, node, depth + 1)",
          },
        ],
      },
      {
        file: "lib/stringify.js",
        originalSha256: "379f22d77bfa1478341ccd49c5e4267464aabcbba03558bab332aac23fc6f23a",
        patchedSha256: "c1285466e686554119ce12703dca1c0e0a4119376bf3e554ad4eeb67d6f71389",
        replacements: [
          {
            before: "  const stringify = (node, parent = {}) => {",
            after:
              "  const stringify = (node, parent = {}, depth = 0) => {\n    if (depth > 128) throw new SyntaxError('Brace nesting exceeds the supported depth');",
          },
          {
            before: "stringify(child)",
            after: "stringify(child, {}, depth + 1)",
          },
        ],
      },
    ],
  },
  {
    name: "http-cache-semantics",
    version: "4.2.0",
    advisoryUrl: "https://github.com/advisories/GHSA-ch52-4w7c-c8xp",
    registryUrl: "https://registry.npmjs.org/http-cache-semantics/-/http-cache-semantics-4.2.0.tgz",
    integrity:
      "sha512-dTxcvPXqPvXBQpq5dUr6mEMJX4oIEFv6bwom3FDwKRDsuIjjJGANqhBuoAn9c1RQJIdAKav33ED65E2ys+87QQ==",
    files: [
      {
        file: "index.js",
        originalSha256: "01b7d66c854b2fe53ac05c98feb6e0d64722ab8898a778e2d2426a8b468d178f",
        patchedSha256: "91ae6a167e8e505a6524aad67551dbbf461c467cb17b41297eb5f74e9240c444",
        replacements: [
          {
            before: "    evaluateRequest(req) {\n        this._assertRequestHasHeaders(req);",
            after:
              "    evaluateRequest(req) {\n        this._assertRequestHasHeaders(req);\n\n        // Never let max-stale or stale-while-revalidate revive an entry\n        // whose freshness was zeroed for cache safety. Revalidate instead.\n        if (!this.storable() || this.maxAge() <= 0) {\n            return this._evaluateRequestMissResult(req);\n        }",
          },
        ],
      },
    ],
  },
] as const;
