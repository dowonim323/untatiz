# WEB KNOWLEDGE BASE

## OVERVIEW
`web/` is the Flask surface area: app factory, auth/session helpers, blueprints, data-shaping services, templates, and small utility helpers.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| App bootstrap | `app.py`, `untatiz_web.py` | Factory, blueprint registration, dev/prod entrypoints |
| Auth/session helpers | `auth.py` | `login_required`, hash/login/logout behavior |
| DB/request helpers | `utils.py` | Flask `g` DB connection, season helpers, formatting |
| HTML routes | `routes/` | Thin blueprints; see child AGENTS |
| Query/data shaping | `services/` | SQL/pandas + cache + template/API payloads |
| UI output | `templates/`, `static/` | Mostly passive assets driven by route/service contracts |

## CONVENTIONS
- `create_app()` is the canonical registration point for blueprints and shared template context.
- `untatiz_web.py` is intentionally thin and path-sensitive; treat it as runtime glue, not a logic hub.
- Session/auth behavior is simple and lives in `auth.py`; admin endpoints use decorators rather than a separate auth framework.
- Many responses are Korean-label presentation objects; keep route/template/service contracts stable.

## ANTI-PATTERNS
- Do not move Flask request/session globals into `app/` backend code.
- Do not bury new startup logic in `untatiz_web.py`; prefer `create_app()`.
- Do not change `/health` semantics casually; Docker depends on it operationally.

## NOTES
- `web/untatiz_web.py` exists partly to make the root-level `web/` package importable from multiple run modes.
- The most common cross-boundary bug in this tree is leaking query/data-shaping logic upward into routes instead of keeping it in services.
- When touching both web and backend code, verify whether the change belongs in `web/services/` first before reaching down into `app/`.
