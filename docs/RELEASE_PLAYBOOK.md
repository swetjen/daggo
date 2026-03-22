# Release Playbook

Canonical process for cutting a DAGGO release.

## Version Rules
- `VERSION` at the repo root is the canonical release version and must include the leading `v`, for example `v0.5.0`.
- The Git tag for the release must match `VERSION` exactly.
- The Go module release version is the Git tag. There is no separate version field in `go.mod`.
- `frontend-web/package.json` must match `VERSION` without the leading `v`.
- The newest changelog heading in `CHANGELOG.md` must use `## vX.Y.Z - YYYY-MM-DD`.

## Release Steps
1. Update `VERSION`.
2. Update `frontend-web/package.json` so its `version` matches `VERSION` without the leading `v`.
3. Add or update the newest `CHANGELOG.md` heading to include both the version and release date.
4. Update any README snippets, examples, and docs affected by the release.
5. Run:

```bash
make gen-all
go test ./...
```

6. Validate the README startup snippet in a fresh throwaway Go module or equivalent clean environment.
7. Validate the canonical downstream app at `/home/incognito/dev/mono/runner`:
- `cd /home/incognito/dev/mono/runner`
- Run `go mod tidy`
- The runner currently uses `replace github.com/swetjen/daggo => ../../daggo`, so this normally updates it to the local DAGGO workspace automatically.
- If the runner now needs source changes to accommodate new DAGGO syntax or APIs, stop the release process, propose the runner changes, and wait for approval before editing `/home/incognito/dev/mono/runner`.
- If no runner source changes are needed, restart or start the canonical runner validation target:
  `systemctl --user restart runner.service || systemctl --user start runner.service`
- If the user service is unavailable, fall back to manual validation from `/home/incognito/dev/mono/runner` with:

```bash
go run .
```

- Treat a successful runner restart/start as a required release validation gate.
8. Start the DAGGO app and confirm:
- the admin UI loads
- built frontend assets load correctly
- `/rpc/docs/` loads
- the displayed DAGGO version matches `VERSION`
9. Commit the release changes.
10. Create a Git tag that exactly matches `VERSION`.
11. Push the commit and tag.

## GitHub Release Automation
- `.github/workflows/release.yml` runs on tags matching `v*`.
- The workflow verifies the pushed tag matches `VERSION`.
- The workflow runs `go test ./...`.
- The workflow builds release archives for supported OS and architecture targets.
- The workflow uploads the release artifacts and `checksums.txt` to the GitHub Release.

## Example

```bash
printf 'v0.6.0\n' > VERSION
# update frontend-web/package.json and CHANGELOG.md
make gen-all
go test ./...
git add VERSION frontend-web/package.json CHANGELOG.md .
git commit -m "release: v0.6.0"
git tag v0.6.0
git push origin main --tags
```
