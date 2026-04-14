"""
Batch migration script — runs run_migration.py for multiple source dashboards.
Automatically copies source to create dest (in the same folder), or reuses an
existing dest found in the migration tracker.

USAGE:
  python3 batch_migrate.py 1722 1800 1900 [--dry-run] [--ini looker.ini]
"""

import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
import looker_sdk
from looker_sdk import models40 as models

# Migration tracker dashboard element (dashboard 2145)
LOG_ELEMENT_ID = "29861"
LOG_ROW_STYLE  = "border: 1px solid #dddddd; padding: 12px;"
BASE_URL       = "https://sentryio.cloud.looker.com/dashboards"


def get_tracker_body(sdk):
    return sdk.dashboard_element(LOG_ELEMENT_ID).body_text or ""


def get_dest_from_tracker(body, source_id):
    match = re.search(
        rf'<td[^>]*>(?:<a[^>]*>)?\s*{re.escape(source_id)}\s*(?:</a>)?</td>'
        rf'\s*<td[^>]*>(?:<a[^>]*>)?\s*(\d+)\s*(?:</a>)?</td>',
        body,
    )
    return match.group(1) if match else None


def upsert_tracker(sdk, source_id, dest_id, dashboard_name, status):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_row = (
        f'    <tr>'
        f'<td style="{LOG_ROW_STYLE}"><a href="{BASE_URL}/{source_id}">{source_id}</a></td>'
        f'<td style="{LOG_ROW_STYLE}"><a href="{BASE_URL}/{dest_id}">{dest_id}</a></td>'
        f'<td style="{LOG_ROW_STYLE}">{dashboard_name}</td>'
        f'<td style="{LOG_ROW_STYLE}">{timestamp}</td>'
        f'<td style="{LOG_ROW_STYLE}">{status}</td>'
        f'</tr>'
    )
    current = get_tracker_body(sdk)
    row_pattern = rf'<tr>\s*<td[^>]*>(?:<a[^>]*>)?\s*{re.escape(source_id)}\s*(?:</a>)?</td>.*?</tr>'
    if re.search(row_pattern, current, flags=re.DOTALL):
        updated = re.sub(row_pattern, new_row, current, flags=re.DOTALL)
    elif "</tbody>" in current:
        updated = current.replace("</tbody>", new_row + "\n  </tbody>")
    else:
        updated = current + "\n" + new_row

    sdk.update_dashboard_element(LOG_ELEMENT_ID, models.WriteDashboardElement(body_text=updated))
    print(f"  ✅ Tracker updated: {source_id} → {dest_id} ({status})")


def get_or_create_dest(sdk, source_id, tracker_body):
    existing = get_dest_from_tracker(tracker_body, source_id)
    if existing:
        print(f"  Already tracked — reusing dest {existing}")
        return existing

    source = sdk.dashboard(source_id)
    title = f"[migrated] {source.title or source_id}"
    created = sdk.create_dashboard(models.WriteDashboard(title=title, folder_id="928"))
    dest_id = str(created.id)
    print(f"  Created blank: '{title}' (dashboard {dest_id})")
    return dest_id


def main():
    p = argparse.ArgumentParser(description="Batch Looker dashboard migration")
    p.add_argument("sources", nargs="+", help="Source dashboard IDs to migrate")
    p.add_argument("--ini", default="looker.ini", help="Path to looker.ini")
    args = p.parse_args()

    sdk = looker_sdk.init40(config_file=args.ini)
    sdk.update_session(models.WriteApiSession(workspace_id="dev"))

    tracker_body = get_tracker_body(sdk)

    for source_id in args.sources:
        print(f"\n{'=' * 50}")
        print(f"Dashboard {source_id}")
        dest_id = get_or_create_dest(sdk, source_id, tracker_body)

        cmd = [sys.executable, "run_migration.py",
               "--source", source_id, "--dest", dest_id, "--ini", args.ini]

        result = subprocess.run(cmd)

        status = "migrated" if result.returncode == 0 else "migrated with errors"
        dashboard_name = sdk.dashboard(dest_id).title or ""
        upsert_tracker(sdk, source_id, dest_id, dashboard_name, status)


if __name__ == "__main__":
    main()
