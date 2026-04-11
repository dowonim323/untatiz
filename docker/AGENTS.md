# DOCKER KNOWLEDGE BASE

## OVERVIEW
`docker/` defines the production runtime model: one container, Supervisor-managed scraper/web processes, and container-path-based app wiring.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Container bootstrap | `entrypoint.sh` | Sets permissions, startup marker, then execs CMD |
| Process model | `supervisord.conf` | Runs scraper loop and Gunicorn web |
| Scraper cadence | `scraper_loop.sh` | Infinite 60-second loop around `python /app/untatiz.py` |

## CONVENTIONS
- Runtime paths are container paths (`/app/...`), not host paths.
- Gunicorn serves `untatiz_web:app` from `/app/web`; scraper runs separately via shell loop.
- `/health` on port 5000 is the shared healthcheck target in Dockerfile/compose.

## ANTI-PATTERNS
- Do not assume one-process-per-container architecture here; Supervisor is intentional.
- Do not hardcode host paths into container scripts.
- Do not change env variable shapes without checking both `docker-compose.yml` and app config readers.

## NOTES
- This repo uses Docker Compose mainly as an operational shell around one multi-process container, not as a microservice split.
- Discord/OpenAI credential wiring is a known mismatch area; audit `entrypoint.sh`, `docker-compose.yml`, and `app/config/settings.py` together.
- The scraper timing model is layered: Supervisor keeps the process alive, `scraper_loop.sh` sleeps every 60s, and `scheduler.py` decides whether work happens.
- If container behavior diverges from local runs, compare `/app` path assumptions first before blaming scraper or Flask code.
