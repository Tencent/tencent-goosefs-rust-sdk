import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "GooseFS SDK",
  tagline: "Rust and Python clients for Tencent GooseFS",
  favicon: "img/favicon.svg",

  // GitHub Pages project site:
  // https://tencent.github.io/tencent-goosefs-rust-sdk/
  url: "https://tencent.github.io",
  baseUrl: "/tencent-goosefs-rust-sdk/",

  organizationName: "Tencent",
  projectName: "tencent-goosefs-rust-sdk",
  deploymentBranch: "gh-pages",
  trailingSlash: false,

  onBrokenLinks: "throw",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  plugins: [
    [
      "@docusaurus/plugin-pwa",
      {
        debug: false,
        offlineModeActivationStrategies: [
          "appInstalled",
          "standalone",
          "queryString",
        ],
        pwaHead: [
          {
            tagName: "link",
            rel: "icon",
            href: "/tencent-goosefs-rust-sdk/img/favicon.svg",
          },
          {
            tagName: "link",
            rel: "manifest",
            href: "/tencent-goosefs-rust-sdk/manifest.json",
          },
          { tagName: "meta", name: "theme-color", content: "#00A4FF" },
        ],
      },
    ],
  ],

  presets: [
    [
      "classic",
      {
        docs: {
          routeBasePath: "/",
          sidebarPath: "./sidebars.ts",
          editUrl:
            "https://github.com/Tencent/tencent-goosefs-rust-sdk/edit/main/website/",
        },
        blog: false,
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: "img/social-card.svg",
    colorMode: {
      defaultMode: "light",
      disableSwitch: true,
    },
    navbar: {
      title: "GooseFS SDK",
      logo: {
        alt: "GooseFS SDK",
        src: "img/logo.svg",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "docsSidebar",
          position: "left",
          label: "Docs",
        },
        {
          href: "https://cloud.tencent.com/document/product/1424",
          label: "GooseFS",
          position: "left",
        },
        {
          href: "https://crates.io/crates/goosefs-sdk",
          label: "crates.io",
          position: "left",
        },
        {
          href: "https://pypi.org/project/goosefs/",
          label: "PyPI",
          position: "left",
        },
        {
          href: "https://github.com/Tencent/tencent-goosefs-rust-sdk",
          position: "right",
          className: "header-github-link",
          "aria-label": "GitHub repository",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            { label: "Introduction", to: "/" },
            { label: "Rust Installation", to: "/user-guide/rust/installation" },
            {
              label: "Python Installation",
              to: "/user-guide/python/installation",
            },
          ],
        },
        {
          title: "Packages",
          items: [
            {
              label: "goosefs-sdk (crates.io)",
              href: "https://crates.io/crates/goosefs-sdk",
            },
            {
              label: "goosefs (PyPI)",
              href: "https://pypi.org/project/goosefs/",
            },
            { label: "docs.rs", href: "https://docs.rs/goosefs-sdk" },
          ],
        },
        {
          title: "More",
          items: [
            {
              label: "GitHub",
              href: "https://github.com/Tencent/tencent-goosefs-rust-sdk",
            },
            {
              label: "GooseFS Product",
              href: "https://cloud.tencent.com/document/product/1424",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Tencent. Licensed under the Apache License, Version 2.0.`,
    },
    prism: {
      theme: prismThemes.vsDark,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ["rust", "toml", "bash", "python", "properties"],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
