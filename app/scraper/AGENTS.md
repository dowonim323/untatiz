# APP/SCRAPER KNOWLEDGE BASE

## OVERVIEW
`app/scraper/` is the most specialized subsystem: Statiz login-aware scraping, Selenium/session management, update scheduling, parsing, and DB write orchestration.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Driver/session rotation | `client.py` | `StatizClient`, account-proxy rotation, cookies, request counts |
| Update cadence | `scheduler.py` | `hourly` / `every_10min` / `every_5min` state machine |
| Main write path | `jobs.py` | Update, backup, normalized-table writes, compatibility hooks |
| HTML/data extraction | `parsers.py` | Statiz-specific assumptions and mappings |

## CONVENTIONS
- Assume Statiz login is required until code proves otherwise.
- Preserve rate limiting, request counting, and account rotation semantics.
- `scheduler.py` state is persisted JSON; maintain backward-compatible keys when changing state shape.
- `client.py` is a wrapper around `statiz_utils.py`; many low-level behaviors still live there.
- Treat year-specific mappings and scraping selectors as fragile contracts.

## ANTI-PATTERNS
- Do not replace Selenium/login-aware flows with requests-only shortcuts.
- Do not bypass `rotate()`, `ensure_ready()`, or request-count resets.
- Do not assume one credentials schema, one proxy mode, or one Chromium path.
- Do not mix unrelated cleanup/refactors into scraper bug fixes; this area is operationally brittle.

## NOTES
- The outer execution loop lives in Docker/supervisor shell scripts; `untatiz.py` itself is a single-run batch entrypoint.
- When debugging scraper failures, inspect `client.py`, `scheduler.py`, and `statiz_utils.py` together before changing behavior.
