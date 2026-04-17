"""
Looker Layout Sync Script
==========================
Repositions ALL tiles on a destination dashboard so the layout matches the
source dashboard:

  • Tiles whose names match a source tile → moved to the source position
  • Tiles on dest with no name match on source → stacked below source content
  • Source dashboard is never modified

Use --tiles to restrict which source tiles are used for matching (tiles not
in the list are still pushed down if they conflict).

USAGE:
  python3 sync_layout.py --source 1261 --dest 2220
  python3 sync_layout.py --source 1261 --dest 2220 --tiles "ARR Changes" "GRR"
  python3 sync_layout.py --source 1261 --dest 2220 --dry-run

CREDENTIALS:
  Set in looker.ini or pass --ini path.
"""

import argparse
import sys
import looker_sdk
from looker_sdk import models40 as models


def parse_args():
    p = argparse.ArgumentParser(
        description="Sync tile sizes and positions from source dashboard to destination"
    )
    p.add_argument("--source",     required=True, help="Source dashboard ID (read-only)")
    p.add_argument("--dest",       required=True, help="Destination dashboard ID (layout updated here)")
    p.add_argument("--tiles",      nargs="+", metavar="TITLE",
                   help="Restrict matching to these tile names. Unmatched dest tiles are still repositioned.")
    p.add_argument("--ini",        default="looker.ini", help="Path to looker.ini (default: ./looker.ini)")
    p.add_argument("--production", action="store_true",
                   help="Run against production (skip dev session)")
    p.add_argument("--dry-run",    action="store_true",
                   help="Show what would change without writing anything")
    return p.parse_args()


def get_layout(sdk, dashboard_id):
    """Return (comp_map, layout_id) where comp_map is {element_id: component}."""
    layout = next(
        (l for l in sdk.dashboard_dashboard_layouts(str(dashboard_id))
         if getattr(l, "active", False)),
        None
    )
    if not layout:
        return {}, None
    return (
        {c.dashboard_element_id: c for c in (layout.dashboard_layout_components or [])},
        str(layout.id)
    )


def pos(comp):
    """Sort key from a layout component — treats None as 9999."""
    if not comp:
        return (9999, 9999)
    return (comp.row or 9999, comp.column or 9999)


def group_by_title(elements):
    """Return {title: [elements...]} preserving order within each group."""
    groups = {}
    for el in elements:
        groups.setdefault(el.title or "", []).append(el)
    return groups


if __name__ == "__main__":
    args = parse_args()
    sdk  = looker_sdk.init40(config_file=args.ini)

    if not args.production:
        sdk.update_session(models.WriteApiSession(workspace_id="dev"))

    # ── fetch elements + layouts ──────────────────────────────────────────────
    src_elements = sdk.dashboard_dashboard_elements(args.source)
    dst_elements = sdk.dashboard_dashboard_elements(args.dest)

    src_layout_map, _             = get_layout(sdk, args.source)
    dst_layout_map, dst_layout_id = get_layout(sdk, args.dest)

    if not dst_layout_id:
        print(f"⚠️  No active layout on destination dashboard {args.dest}. Exiting.")
        sys.exit(1)

    # Sort both sets by position so duplicate-named tiles match in visual order
    src_sorted = sorted(src_elements, key=lambda e: pos(src_layout_map.get(e.id)))
    dst_sorted = sorted(dst_elements, key=lambda e: pos(dst_layout_map.get(e.id)))

    src_by_title = group_by_title(src_sorted)
    dst_by_title = group_by_title(dst_sorted)

    # ── determine which source titles to match against ────────────────────────
    match_titles = set(args.tiles) if args.tiles else set(src_by_title.keys()) - {""}

    # ── build the update plan ─────────────────────────────────────────────────
    # matched_updates: list of (dst_comp, dst_el, src_comp)
    # unmatched_dst_ids: dest elements with no source match
    matched_updates  = []
    matched_dst_ids  = set()
    unmatched_warnings = []

    for title in match_titles:
        src_group = src_by_title.get(title, [])
        dst_group = dst_by_title.get(title, [])

        if not src_group:
            unmatched_warnings.append(f"  ⚠️  '{title}' not found on source — skipped")
            continue
        if not dst_group:
            unmatched_warnings.append(f"  ⚠️  '{title}' not found on destination — skipped")
            continue

        pairs = list(zip(src_group, dst_group))
        if len(src_group) != len(dst_group):
            unmatched_warnings.append(
                f"  ⚠️  '{title}': {len(src_group)} on source, {len(dst_group)} on dest "
                f"— matched first {len(pairs)}"
            )

        for src_el, dst_el in pairs:
            src_comp = src_layout_map.get(src_el.id)
            dst_comp = dst_layout_map.get(dst_el.id)
            if src_comp and dst_comp:
                matched_updates.append((dst_comp, dst_el, src_comp))
                matched_dst_ids.add(dst_el.id)

    # Dest tiles with no source match — will be stacked below source content
    unmatched_dst = [
        el for el in dst_sorted
        if el.id not in matched_dst_ids and el.id in dst_layout_map
    ]

    # Find the bottom edge of all source tile positions
    src_bottom = max(
        ((c.row or 0) + (c.height or 0) for c in src_layout_map.values()),
        default=0
    )
    next_row = src_bottom + 2  # leave a gap below source content

    unmatched_updates = []
    for el in unmatched_dst:
        comp = dst_layout_map[el.id]
        h = comp.height or 6
        unmatched_updates.append((comp, el, next_row))
        next_row += h

    # ── print plan ────────────────────────────────────────────────────────────
    print(f"\nSource dashboard {args.source} → Destination dashboard {args.dest}")
    if args.dry_run:
        print("[DRY RUN — no writes]\n")

    for w in unmatched_warnings:
        print(w)

    if args.dry_run:
        print(f"Matched tiles ({len(matched_updates)}):")
        for dst_comp, dst_el, src_comp in matched_updates:
            title = dst_el.title or "(untitled)"
            changed = (
                dst_comp.row    != src_comp.row    or
                dst_comp.column != src_comp.column or
                dst_comp.width  != src_comp.width  or
                dst_comp.height != src_comp.height
            )
            if changed:
                print(f"  '{title}':")
                if dst_comp.row != src_comp.row or dst_comp.column != src_comp.column:
                    print(f"    position: ({dst_comp.row},{dst_comp.column}) → ({src_comp.row},{src_comp.column})")
                if dst_comp.width != src_comp.width or dst_comp.height != src_comp.height:
                    print(f"    size:     {dst_comp.width}w×{dst_comp.height}h → {src_comp.width}w×{src_comp.height}h")
            else:
                print(f"  '{title}': already matches")

        if unmatched_updates:
            print(f"\nUnmatched dest tiles — pushed below row {src_bottom} ({len(unmatched_updates)}):")
            for comp, el, new_row in unmatched_updates:
                title = el.title or "(untitled)"
                print(f"  '{title}': row {comp.row or '?'} → {new_row}")

        total = len(matched_updates) + len(unmatched_updates)
        print(f"\n✓ {total} tile(s) would be repositioned")
        sys.exit(0)

    # ── apply matched updates ─────────────────────────────────────────────────
    print(f"Syncing {len(matched_updates)} matched tile(s)...")
    for dst_comp, dst_el, src_comp in matched_updates:
        sdk.update_dashboard_layout_component(
            str(dst_comp.id),
            models.WriteDashboardLayoutComponent(
                dashboard_layout_id=dst_layout_id,
                dashboard_element_id=str(dst_el.id),
                row=src_comp.row,
                column=src_comp.column,
                width=src_comp.width,
                height=src_comp.height,
            )
        )
        print(f"  ✓ '{dst_el.title or '(untitled)'}' → "
              f"{src_comp.width}w×{src_comp.height}h at ({src_comp.row},{src_comp.column})")

    # ── push unmatched tiles below source content ─────────────────────────────
    if unmatched_updates:
        print(f"\nPushing {len(unmatched_updates)} unmatched dest tile(s) below row {src_bottom}...")
        for comp, el, new_row in unmatched_updates:
            sdk.update_dashboard_layout_component(
                str(comp.id),
                models.WriteDashboardLayoutComponent(
                    dashboard_layout_id=dst_layout_id,
                    dashboard_element_id=str(el.id),
                    row=new_row,
                    column=comp.column or 0,
                    width=comp.width or 12,
                    height=comp.height or 6,
                )
            )
            print(f"  ✓ '{el.title or '(untitled)'}' pushed to row {new_row}")

    total = len(matched_updates) + len(unmatched_updates)
    print(f"\n✓ Done: {total} tile(s) repositioned")
