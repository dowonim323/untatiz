# WEB/SERVICES KNOWLEDGE BASE

## OVERVIEW
`web/services/` is the presentation-data layer: SQL/pandas queries, cache usage, sorting/filtering, and return values shaped for templates or JSON routes.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Home page payloads | `index_service.py` | Update info, standings, news lookup |
| League/team/player pages | `league_service.py`, `team_service.py`, `player_service.py` | Query/filter/sort + output row shaping |
| Roster/GOAT/BOAT | `roster_service.py`, `gboat_service.py` | Transaction and daily-record data prep |

## CONVENTIONS
- Services return template/API-ready dict/list structures, often with Korean display labels and formatted numeric strings.
- Mixing `sqlite3` cursors and `pandas.read_sql_query` is already part of this layer; match nearby patterns.
- Cache usage lives here frequently (`cached_query`, TTL constants, namespaces).
- Team ordering and FA handling (`'퐈'`) are part of output semantics, not incidental formatting.

## ANTI-PATTERNS
- Do not use Flask request/session globals inside services.
- Do not leak raw DB rows when existing callers expect shaped display payloads.
- Do not change cache namespaces/TTLs casually when extending existing flows.
- Do not break date/team/display-name mapping contracts without updating callers/templates.

## NOTES
- This layer already mixes low-level SQLite calls and pandas queries; consistency with neighboring files matters more than forcing one access style.
- Hardcoded host paths still exist in a few service functions; prefer config-aware paths for new code instead of copying those patterns.
- Service outputs are part of the UI contract; small label or field-name changes can break templates and client-side assumptions.
- `index_service.py` and `player_service.py` are good exemplars for the existing "query then shape for display" pattern.
- When changing service payloads, inspect the paired route and template together; this layer is tightly coupled to presentation contracts.
