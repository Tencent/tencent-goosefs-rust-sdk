# GooseFS SDK Website

Docusaurus documentation site for the GooseFS Rust / Python SDK.

## Local development

```bash
cd website
npm install
npm start
```

Open http://localhost:3000/tencent-goosefs-rust-sdk/.

## Build

```bash
npm run build
npm run serve
```

## GitHub Pages

CI workflow [`.github/workflows/docs.yml`](../.github/workflows/docs.yml):

- **PR / push touching `website/`** — builds the site only (`npm run build`)
- **Push to `main`** — builds and deploys to GitHub Pages

Code CI (`ci.yml`, `ci_integration.yml`) uses `paths-ignore: website/**`, so
docs-only changes do **not** run Rust/Python unit or integration tests.

Published site:

https://tencent.github.io/tencent-goosefs-rust-sdk/

Enable **Settings → Pages → Source: GitHub Actions** once on the repository.
