// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer').themes.github;
const darkCodeTheme = require('prism-react-renderer').themes.dracula;
const variableInjector = require('./variable-inject')

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'LakeSoul - An Opensource Cloud Native Realtime Lakehouse Framework',
  tagline: 'A Project under incubation at Linux Foundation AI & Data',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://lakesoul-io.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'lakesoul-io', // Usually your GitHub org/user name.
  projectName: 'lakesoul-io.github.io', // Usually your repo name.
  deploymentBranch: 'main',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'throw',

  trailingSlash: false,

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'zh-Hans'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/lakesoul-io/LakeSoul/tree/main/website/',
          remarkPlugins: [
            [variableInjector, {
              replacements: {
                VERSION: '2.6.0',
                CURRENT_YEAR: new Date().getFullYear()
              }
            }]
          ],
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/lakesoul-io/LakeSoul/tree/main/website/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/LakeSoul_Horizontal_White.png',
      navbar: {
        title: 'LakeSoul',
        logo: {
          alt: 'LakeSoul Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Docs',
          },
          {to: '/blog', label: 'Blog', position: 'left'},
          {
            type: 'localeDropdown',
            position: 'right',
          },
          {
            href: 'https://github.com/lakesoul-io/LakeSoul',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      colorMode: {
        disableSwitch: true,
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Getting Started',
                to: '/docs/Getting%20Started/setup-local-env',
              },
              {
                label: 'Docs',
                to: '/docs/Usage%20Docs/setup-meta-env',
              },
              {
                label: 'Tutorials',
                to: '/docs/Tutorials/consume-cdc-via-spark-streaming',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Discord',
                href: 'https://discord.gg/WJrHKq4BPf',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/lakesoul',
              },
              {
                label: 'LakeSoul Announce',
                href: 'https://lists.lfaidata.foundation/g/lakesoul-announce',
              },
              {
                label: 'LakeSoul Technical-Discuss',
                href: 'https://lists.lfaidata.foundation/g/lakesoul-technical-discuss',
              },
              {
                label: 'LakeSoul TSC',
                href: 'https://lists.lfaidata.foundation/g/lakesoul-tsc',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/lakesoul-io/lakesoul',
              },
            ],
          },
        ],
        copyright: `<div class='customCopyright'>Copyright ©  ${new Date().getFullYear()}  LakeSoul The Linux Foundation®. All rights reserved. The Linux Foundation has registered trademarks and uses trademarks. <br> For a list of trademarks of The Linux Foundation, please see our <a href='https://www.linuxfoundation.org/legal/trademark-usage' target='_blank'>Trademark Usage</a> page. Linux is a registered trademark of Linus Torvalds. <a href="https://www.linuxfoundation.org/legal/privacy-policy" target='_blank'>Privacy Policy</a> and <a href="https://www.linuxfoundation.org/legal/terms" target='_blank'>Terms of Use</a></div>`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: [
          'java',
          'scala',
        ],
      },
    }),
  plugins: ['./canvas-loader.js'],
};

module.exports = config;
