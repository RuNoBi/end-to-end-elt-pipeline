# Airbyte config-as-code (localhost)

Keep **sync definition** in git even when the UI was used to create the connection.

## Workflow

1. Configure source / destination / connection in Airbyte UI once.
2. Export connection JSON:

   ```bash
   cd airbyte-platform
   ./scripts/export-connection.sh
   ```

3. Commit sanitized export to `config/connections/` (optional) — keep **connection UUID** in `airflow-platform/.env` only.

## Files

| File | Purpose |
|------|---------|
| `connection.template.json` | Retail Postgres → warehouse |
| `connection.sap_chemicals.template.json` | SAP schema → warehouse |
| `connection.api_countries.template.json` | HTTP API countries → `src_api_countries` |
| `connections/*.json` | Exported configs (add to git when ready) |

## Production path

Replace manual export with [Octavia CLI](https://github.com/airbytehq/airbyte/tree/master/octavia-cli) or Terraform Airbyte provider when moving off localhost.
