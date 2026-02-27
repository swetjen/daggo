# playwright_decisions

Resolved decisions:
- Websocket protocol is explicit via `PLAYWRIGHT_WS_PROTOCOL` (`playwright` default, `cdp` optional).
- For headed/CDP scraping, missing remote browser contexts default to auto-create; set `PLAYWRIGHT_REQUIRE_EXISTING_CONTEXT=true` to fail instead.
- Runtime install policy is explicit: production images must pre-install browser binaries (for example `playwright install chromium` during build/bootstrap).

Open issues:
- None.
