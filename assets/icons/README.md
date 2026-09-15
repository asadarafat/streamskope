# StreamSkope application icon

The blue badge with a white S-shaped stream identifies the application.
Its colors do not represent connection state.

Source: [streamskope.svg](../../src/ui/assets/streamskope.svg). The workbench and
favicon use that SVG directly. PNG, ICNS and ICO files are generated copies used
by desktop packaging, not independent artwork. Licensed with the project.

To regenerate after editing the SVG:

```bash
npx --no-install playwright install chromium
node tools/generate-application-icons.mjs
npx --no-install vitest run --config config/vitest.config.ts test/unit/application-identity.test.ts
```

Commit the source, generated files and `generation.json` together. Ordinary
package builds use the checked-in files and do not need Chromium installed.
Rebuild the macOS/Windows package to update its Finder/Explorer/Dock icon; a
renderer refresh alone cannot replace an already-built application's icon.
