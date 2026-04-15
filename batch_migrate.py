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
from bs4 import BeautifulSoup

# Migration tracker dashboard element (dashboard 2145)
LOG_ELEMENT_ID = "29861"
LOG_ROW_STYLE  = "border: 1px solid #dddddd; padding: 12px;"
BASE_URL       = "https://sentryio.cloud.looker.com/dashboards"


def get_tracker_body(sdk):
    return sdk.dashboard_element(LOG_ELEMENT_ID).body_text or ""


def get_dest_from_tracker(body, source_id):
    soup = BeautifulSoup(body, "html.parser")
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if cells and cells[0].get_text(strip=True) == source_id:
            return cells[1].get_text(strip=True) if len(cells) > 1 else None
    return None


def upsert_tracker(sdk, source_id, dest_id, dashboard_name, status):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    current = get_tracker_body(sdk)
    if not current.strip():
        current = "<table><tbody></tbody></table>"
    soup = BeautifulSoup(current, "html.parser")

    new_row = BeautifulSoup(
        f'<tr>'
        f'<td style="{LOG_ROW_STYLE}"><a href="{BASE_URL}/{source_id}">{source_id}</a></td>'
        f'<td style="{LOG_ROW_STYLE}"><a href="{BASE_URL}/{dest_id}">{dest_id}</a></td>'
        f'<td style="{LOG_ROW_STYLE}">{dashboard_name}</td>'
        f'<td style="{LOG_ROW_STYLE}">{timestamp}</td>'
        f'<td style="{LOG_ROW_STYLE}">{status}</td>'
        f'</tr>',
        "html.parser",
    ).find("tr")

    existing = next(
        (row for row in soup.find_all("tr")
         if row.find_all("td") and row.find_all("td")[0].get_text(strip=True) == source_id),
        None,
    )
    if existing:
        existing.replace_with(new_row)
    elif soup.find("tbody"):
        soup.find("tbody").append(new_row)
    else:
        soup.append(new_row)

    sdk.update_dashboard_element(LOG_ELEMENT_ID, models.WriteDashboardElement(body_text=str(soup)))
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
    p.add_argument("--explore-from", nargs="+", default=None, metavar="EXPLORE", help="Old explore name(s) to remap (forwarded to run_migration.py)")
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
        if args.explore_from:
            cmd += ["--explore-from"] + args.explore_from

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(result.stdout, end="")

        pct_match = re.search(r"(\d+)/(\d+) tiles migrated cleanly \((\d+)%\)", result.stdout)
        if pct_match:
            status = f"migrated ({pct_match.group(3)}%)"
        elif result.returncode != 0:
            status = "script failure"
        else:
            status = "migrated"
        dashboard_name = sdk.dashboard(dest_id).title or ""
        upsert_tracker(sdk, source_id, dest_id, dashboard_name, status)


if __name__ == "__main__":
    main()
