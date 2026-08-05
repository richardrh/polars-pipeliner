import { defineConfig } from "vitepress";

export default defineConfig({
  title: "polars-pipeliner",
  description: "Build and run dependency-ordered Polars LazyFrame queries",
  base: "/polars-pipeliner/",
  lastUpdated: true,
  sitemap: {
    hostname: "https://richardrh.github.io/polars-pipeliner/",
  },
  themeConfig: {
    nav: [
      { text: "Guide", link: "/getting-started" },
      { text: "Concepts", link: "/concepts/models" },
      { text: "Reference", link: "/reference/api" },
      { text: "Comparison", link: "/comparison" },
    ],
    sidebar: [
      {
        text: "Guide",
        items: [
          { text: "Introduction", link: "/" },
          { text: "Getting started", link: "/getting-started" },
        ],
      },
      {
        text: "Concepts",
        items: [
          { text: "Model types", link: "/concepts/models" },
          { text: "Schemas and validation", link: "/concepts/schemas" },
          {
            text: "Discovery and execution",
            link: "/concepts/execution",
          },
          { text: "Sources and outputs", link: "/concepts/io" },
        ],
      },
      {
        text: "Reference",
        items: [
          { text: "Public API", link: "/reference/api" },
          { text: "Configuration", link: "/reference/configuration" },
        ],
      },
      {
        text: "Comparison",
        items: [
          { text: "Compared with Lea and dbt", link: "/comparison" },
        ],
      },
    ],
    search: {
      provider: "local",
    },
    socialLinks: [
      {
        icon: "github",
        link: "https://github.com/richardrh/polars-pipeliner",
      },
    ],
    editLink: {
      pattern:
        "https://github.com/richardrh/polars-pipeliner/edit/master/docs/:path",
      text: "Edit this page on GitHub",
    },
    outline: [2, 3],
  },
});
