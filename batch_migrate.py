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
TABLE_STYLE    = "border-collapse: collapse; width: 100%; font-family: Roboto;"
HEADER_STYLE   = "border: 1px solid #dddddd; text-align: left; padding: 12px;"
LOG_ROW_STYLE  = "border: 1px solid #dddddd; text-align: left; padding: 12px;"
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


def upsert_tracker(sdk, source_id, dest_id, dashboard_name, status, total_tiles="", broken_tiles=""):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    current = get_tracker_body(sdk)
    if not current.strip():
        current = (
            f'<table style="{TABLE_STYLE}">'
            f'<thead><tr style="background-color: #f2f2f2;">'
            f'<th style="{HEADER_STYLE}">Source ID</th>'
            f'<th style="{HEADER_STYLE}">Dest ID</th>'
            f'<th style="{HEADER_STYLE}">Dashboard Name</th>'
            f'<th style="{HEADER_STYLE}">Timestamp</th>'
            f'<th style="{HEADER_STYLE}">Status</th>'
            f'<th style="{HEADER_STYLE}">Total Tiles</th>'
            f'<th style="{HEADER_STYLE}">Broken Tiles</th>'
            f'</tr></thead>'
            f'<tbody></tbody>'
            f'</table>'
        )
    soup = BeautifulSoup(current, "html.parser")

    # Ensure table and thead have correct styles and new columns
    table = soup.find("table")
    if table and not table.get("style"):
        table["style"] = TABLE_STYLE
    thead_row = soup.find("thead").find("tr") if soup.find("thead") else None
    if thead_row:
        if not thead_row.get("style"):
            thead_row["style"] = "background-color: #f2f2f2;"
        existing_headers = [th.get_text(strip=True) for th in thead_row.find_all("th")]
        for header in ("Total Tiles", "Broken Tiles"):
            if header not in existing_headers:
                new_th = BeautifulSoup(f'<th style="{HEADER_STYLE}">{header}</th>', "html.parser").find("th")
                thead_row.append(new_th)

    new_row = BeautifulSoup(
        f'<tr>'
        f'<td style="{LOG_ROW_STYLE}"><a href="{BASE_URL}/{source_id}">{source_id}</a></td>'
        f'<td style="{LOG_ROW_STYLE}"><a href="{BASE_URL}/{dest_id}">{dest_id}</a></td>'
        f'<td style="{LOG_ROW_STYLE}">{dashboard_name}</td>'
        f'<td style="{LOG_ROW_STYLE}">{timestamp}</td>'
        f'<td style="{LOG_ROW_STYLE}">{status}</td>'
        f'<td style="{LOG_ROW_STYLE}">{total_tiles}</td>'
        f'<td style="{LOG_ROW_STYLE}">{broken_tiles}</td>'
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

        pct_match    = re.search(r"(\d+)/(\d+) tiles migrated cleanly \((\d+)%\)", result.stdout)
        total_match  = re.search(r"(\d+) with queries", result.stdout)
        broken_match = re.search(r"(\d+) tile\(s\) with unmapped fields", result.stdout)

        if pct_match:
            status = f"migrated ({pct_match.group(3)}%)"
        elif result.returncode != 0:
            status = "script failure"
        else:
            status = "migrated"

        total_tiles  = total_match.group(1)  if total_match  else ""
        broken_tiles = broken_match.group(1) if broken_match else "0" if total_match else ""

        dashboard_name = sdk.dashboard(dest_id).title or ""
        upsert_tracker(sdk, source_id, dest_id, dashboard_name, status, total_tiles, broken_tiles)


if __name__ == "__main__":
    main()
