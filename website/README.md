<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

# Website

The LakeSoul website is built with [Docusaurus 3](https://docusaurus.io/) and
deployed to GitHub Pages.

## Install

```bash
npm install
```

## Local development

```bash
npm run start
```

The dev server serves **one locale at a time**. The locale dropdown in the
navbar only works in a production build: in dev, switching to the other locale
returns 404.

- English: `npm run start`
- Chinese: `npm run start:zh`, then open `http://localhost:3000/zh-Hans/`

To preview both locales and use the switcher:

```bash
npm run build
npm run serve
```

## Build

```bash
npm run build
```

The static site is written to `build/` (`build/` for English and `build/zh-Hans/`
for Chinese). Serve it locally with:

```bash
npm run serve
```

## Docs

- Content: `docs/` (English), `i18n/zh-Hans/docusaurus-plugin-content-docs/current/`
  (Chinese). Keep the relative paths in sync when moving a page.
- Redirects for old URLs: `redirects.js`.
- Sidebar categories: `_category_.json` in each directory; page order:
  `sidebar_position` front matter.
