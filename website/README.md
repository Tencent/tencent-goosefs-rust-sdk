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

CI workflow [`.github/workflows/docs.yml`](../.github/workflows/docs.yml) builds on pushes to `main` that touch `website/` and deploys to:

https://tencent.github.io/tencent-goosefs-rust-sdk/

Enable **Settings → Pages → Source: GitHub Actions** once on the repository.
