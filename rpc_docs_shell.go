package daggo

const rpcDocsHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DAGGO RPC Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  <style>
    :root {
      --bg: #0b0f14;
      --panel: #11161c;
      --line: #273142;
      --text: #e6edf3;
      --muted: #9ba7b4;
    }
    html, body {
      margin: 0;
      height: 100%;
      background: var(--bg);
      color: var(--text);
      font-family: system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }
    .docs-shell {
      min-height: 100%;
      display: flex;
      flex-direction: column;
    }
    .docs-topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 14px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    .docs-title {
      font-size: 14px;
      font-weight: 600;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }
    .docs-topbar a {
      color: var(--muted);
      font-size: 12px;
      text-decoration: none;
    }
    .docs-topbar a:hover {
      color: var(--text);
    }
    #swagger-ui {
      flex: 1;
      min-height: 0;
    }
    .swagger-ui {
      margin: 0;
      padding: 0;
    }
    .swagger-ui .information-container.wrapper {
      margin: 0;
      padding: 12px 16px 8px;
    }
  </style>
</head>
<body>
  <div class="docs-shell">
    <div class="docs-topbar">
      <div class="docs-title">DAGGO RPC Docs</div>
      <a href="/rpc/openapi.json" target="_blank" rel="noreferrer">OpenAPI JSON</a>
    </div>
    <div id="swagger-ui"></div>
  </div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: "/rpc/openapi.json",
      dom_id: "#swagger-ui",
      deepLinking: true,
      tryItOutEnabled: true,
      displayRequestDuration: true,
      presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIStandalonePreset
      ],
      layout: "BaseLayout"
    })
  </script>
</body>
</html>
`
