# WEB/ROUTES KNOWLEDGE BASE

## OVERVIEW
`web/routes/` holds Blueprint controllers. Routes should stay thin: parse request input, call services/helpers, then render templates or return JSON.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Home + health | `index.py` | `/` and deployment-critical `/health` |
| Standings/team/player pages | `league.py`, `team.py`, `player.py` | Query-param normalization + template rendering |
| Transactions/admin | `roster.py`, `admin.py` | Form/JSON handling, auth-protected mutations |
| JSON endpoints | `api.py` | Status/search/graph/table endpoints |

## CONVENTIONS
- One file per blueprint/domain area.
- Prefer delegating SQL/data shaping to `web/services/` or shared helpers.
- Use `login_required` for admin/mutation endpoints rather than repeating auth checks.
- Keep parameter parsing and response shaping explicit; existing code favors straightforward route functions over abstractions.

## ANTI-PATTERNS
- Do not put heavy SQL, caching policy, or dataframe wrangling directly in routes.
- Do not mix HTML-render and JSON semantics in one branch unless the file already does so intentionally.
- Do not weaken auth checks on admin/cache/season/draft endpoints.
- Do not change `/health` from scraper-aware monitoring to a bare ping.

## NOTES
- Query parameters are normalized close to the route layer, then passed to services in simple scalar/list forms.
- `admin.py` and `roster.py` are the mutation-heavy route files; review their auth and response shape before editing adjacent endpoints.
- `index.py` is not just a landing page file; it also owns the deployment-facing health endpoint.
- If a route needs new caching behavior, that decision usually belongs in the corresponding service helper instead of the blueprint itself.
