# APP/SCRAPER KNOWLEDGE BASE

## OVERVIEW
`app/scraper/` is the most specialized subsystem: MLBPARK requests-based scraping, update scheduling, parser normalization, and DB write orchestration.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Driver/session handling | `client.py` | Compatibility wrapper around the MLBPARK requests session, cache, request counts |
| Update cadence | `scheduler.py` | `hourly` / `every_30min` / `every_5min` state machine |
| Main write path | `jobs.py` | Update, backup, normalized-table writes, compatibility hooks |
| Data extraction | `parsers.py` | MLBPARK endpoint mappings and legacy DataFrame shaping |

## CONVENTIONS
- Preserve rate limiting, request counting, and account rotation semantics.
- `scheduler.py` state is persisted JSON; maintain backward-compatible keys when changing state shape.
- `client.py` keeps the legacy `StatizClient` surface for compatibility, but the live runtime now uses MLBPARK requests.
- Treat team-code mappings, position mappings, and MLBPARK payload shapes as fragile contracts.

## ANTI-PATTERNS
- Do not bypass `rotate()`, `ensure_ready()`, or request-count resets.
- Do not assume MLBPARK payload fields or query parameters are permanently stable without checking live responses.
- Do not mix unrelated cleanup/refactors into scraper bug fixes; this area is operationally brittle.

## NOTES
- The outer execution loop lives in Docker/supervisor shell scripts; `untatiz.py` itself is a single-run batch entrypoint.
- When debugging scraper failures, inspect `client.py`, `parsers.py`, and `scheduler.py` together before changing behavior.
