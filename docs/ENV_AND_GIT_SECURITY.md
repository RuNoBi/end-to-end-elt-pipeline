# Environment variables & Git security

## File roles

| File | Git | Purpose |
|------|-----|---------|
| `.env` | **Ignored** | Real credentials your Docker/dbt stack reads today |
| `.env.example` | **Tracked** | Template with placeholders for collaborators |

Docker Compose loads secrets via `env_file: .env` in each service folder. **No change to compose files is required** — keep your existing `.env` files locally.

## Per-service locations

```
source-postgres/.env          ← POSTGRES_* for source DB
warehouse-postgres/.env     ← POSTGRES_* for warehouse (1M rows live here)
airbyte-platform/.env       ← Airbyte internal + worker config
dbt-warehouse/.env          ← DBT_WAREHOUSE_* (must match warehouse .env user/password)
```

After cloning on a new machine:

```bash
cp source-postgres/.env.example source-postgres/.env
cp warehouse-postgres/.env.example warehouse-postgres/.env
cp airbyte-platform/.env.example airbyte-platform/.env
cp dbt-warehouse/.env.example dbt-warehouse/.env
# Edit each .env with real passwords before docker compose up
```

**Important:** `dbt-warehouse` warehouse password must match `warehouse-postgres/.env`.

## Safe to push (private or public repo)

- `.env.example` (all modules)
- `docker-compose.yml` / `docker-compose.yaml`
- dbt `models/`, `macros/`, `dbt_project.yml`, `profiles.yml` (uses `env_var()` — no hardcoded secrets)
- `README.md`, docs, SQL, init scripts without secrets

## Never push

- Any `.env` file
- `dbt-warehouse/target/`, `logs/`, `dbt_packages/`
- Local DB data directories (`pgdata/`, `airbyte_data/`, etc.)
- Docker volume bind-mount folders with real data

## Unstage secrets accidentally added

From repository root:

```bash
# Remove from Git index only (keeps files on disk)
git rm --cached -r --ignore-unmatch \
  source-postgres/.env \
  warehouse-postgres/.env \
  airbyte-platform/.env \
  dbt-warehouse/.env

# If you added any .env variant
git rm --cached -r --ignore-unmatch '**/.env' '**/.env.local' '**/.env.production'

# Re-add templates
git add '**/.env.example'

# Verify .env is ignored
git check-ignore -v warehouse-postgres/.env
```

If secrets were **already committed** to a remote branch, rotate passwords and use `git filter-repo` or GitHub secret scanning — removing from a new commit is not enough for history.

## Verify before push

```bash
git status
git diff --cached
git ls-files | grep -E '\.env$'    # should return nothing
git ls-files | grep '\.env.example' # should list templates
```

## Data preservation

Stopping containers (`docker compose stop`) does **not** delete volume data. Your 1M rows remain in Docker volumes `warehouse_db_data`, `source_db_data`, `airbyte_data`, etc., as long as you do not run `docker volume rm` or `docker compose down -v`.
