# APP/CORE KNOWLEDGE BASE

## OVERVIEW
`app/core/` is the shared infrastructure layer: SQLite access, cache primitives, schema helpers, draft loading, logging, and migration tooling.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Reusable DB access | `db.py` | `DatabaseManager`, FK PRAGMA, pandas SQL helpers |
| Cache behavior | `cache.py` | Cache decorators, invalidation after updates |
| Schema work | `schema.py` | Normalized schema definitions/helpers |
| Migration boundary | `migrate.py` | Legacy-wide to long-format conversion, verification paths |
| Draft ingestion | `draft_loader.py` | Canonical slot naming and draft import rules |
| Shared logging/time utils | `logging.py`, `utils.py` | Repo-level helpers |

## CONVENTIONS
- Use `DatabaseManager.connection()` for reusable DB helpers so FK enforcement stays on.
- `db.py` still exposes legacy convenience wrappers; preserve them unless you are intentionally removing compatibility.
- `migrate.py` is special-purpose migration code; expect explicit verification and transition logic.
- `draft_loader.py` is the canonical place for draft slot normalization (`용타`, `용투1`, `2차1R`, etc.).

## ANTI-PATTERNS
- Do not open new raw SQLite connections in core helpers unless there is a clear reason.
- Do not embed feature-specific web formatting here.
- Do not change migration semantics without checking legacy table/state expectations.
- Do not invent new draft slot names when canonical ones already exist.

## NOTES
- `migrate.py` and parts of `db.py` deliberately preserve older data shapes; prefer additive changes over sweeping cleanup here.
- If a change affects both `core/` and `web/services/`, keep reusable primitives here and presentation formatting in `web/services/`.
