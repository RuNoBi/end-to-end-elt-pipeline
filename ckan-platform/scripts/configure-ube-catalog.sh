#!/usr/bin/env bash
# Post-bootstrap: UBE org branding, Data Explorer views on datastore resources.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CLI_ORG="${CKAN_ORGANIZATION:-}"
if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  source .env
fi

CKAN_URL="${CKAN_SITE_URL:-http://localhost:5001}"
TOKEN="${CKAN_API_TOKEN:-}"
ORG="${CLI_ORG:-${CKAN_ORGANIZATION:-ube-group-thailand}}"
ORG_TITLE="${CKAN_ORGANIZATION_TITLE:-UBE Group Thailand}"
ORG_NOTES="${CKAN_ORGANIZATION_NOTES:-Official Gold datamarts. Open a dataset and use **Data Explorer** to preview rows.}"

if [[ -z "${TOKEN}" ]]; then
  echo "ERROR: CKAN_API_TOKEN missing. Run ./scripts/bootstrap-ckan.sh first."
  exit 1
fi

export CKAN_URL TOKEN ORG ORG_TITLE ORG_NOTES

python3 <<'PY'
import json
import os
import urllib.request

base = os.environ["CKAN_URL"].rstrip("/") + "/api/3/action/"
token = os.environ["TOKEN"]
org = os.environ["ORG"]
org_title = os.environ["ORG_TITLE"]
org_notes = os.environ["ORG_NOTES"]


def api(action: str, payload: dict, *, ok_if_exists: bool = False) -> dict | None:
    req = urllib.request.Request(
        base + action,
        data=json.dumps(payload).encode(),
        headers={"Authorization": token, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.load(resp)
    except urllib.error.HTTPError as exc:
        try:
            body = json.load(exc)
        except Exception:
            body = {}
        err = body.get("error") if isinstance(body, dict) else body
        if ok_if_exists and exc.code in (409, 400):
            err_text = json.dumps(err).lower()
            if "already exists" in err_text or "already in use" in err_text:
                return None
        raise RuntimeError(f"{action} HTTP {exc.code}: {err}") from exc
    if not body.get("success"):
        err = body.get("error")
        if ok_if_exists and isinstance(err, dict) and "already exists" in str(
            err.get("message", "")
        ).lower():
            return None
        raise RuntimeError(f"{action} failed: {err}")
    return body.get("result")


logo_url = os.environ["CKAN_URL"].rstrip("/") + "/images/ube-logo.png"
print(f"Ensuring organization {org} ...")
if api(
    "organization_create",
    {
        "name": org,
        "title": org_title,
        "notes": org_notes,
        "image_url": logo_url,
    },
    ok_if_exists=True,
) is None:
    api(
        "organization_patch",
        {"id": org, "title": org_title, "notes": org_notes, "image_url": logo_url},
    )

legacy = "de-poc-data"
if org != legacy:
    print(f"Migrating packages from {legacy} to {org} (if any) ...")
    search = api("package_search", {"fq": f"organization:{legacy}", "rows": 100}) or {}
    for pkg in search.get("results") or []:
        api("package_patch", {"id": pkg["name"], "owner_org": org})
        print(f"  moved {pkg['name']}")

print("Adding Data Explorer (datatables) views ...")
search = api("package_search", {"fq": f"organization:{org}", "rows": 100}) or {}
for pkg in search.get("results") or []:
    for res in pkg.get("resources") or []:
        if res.get("url_type") != "datastore":
            continue
        rid = res["id"]
        views = api("resource_view_list", {"id": rid}) or []
        if any(v.get("view_type") == "datatables_view" for v in views):
            continue
        api(
            "resource_view_create",
            {
                "resource_id": rid,
                "view_type": "datatables_view",
                "title": "Data Explorer",
            },
        )
        print(f"  view created for resource {rid}")

# Brand legacy PoC org slug if datasets still live there
legacy = "de-poc-data"
if legacy != org:
    try:
        api(
            "organization_patch",
            {
                "id": legacy,
                "title": org_title + " (legacy)",
                "notes": "Legacy slug — new datasets are under **" + org + "**.",
            },
        )
    except RuntimeError:
        pass

print(f"Done. Open {os.environ['CKAN_URL']}/organization/about/{org}")
PY
