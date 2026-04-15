"""
Pre-migration field checks.

  python3 run_migration.py --check --source 1722
  python3 run_migration.py --batch 1722 1800 1900
"""

import json
import sys
from collections import defaultdict

from mappings import OLD_EXPLORE, NEW_MODEL, NEW_EXPLORE, NEW_EXPLORE_2, FIELD_MAPS

# Combined field map across all old→new explore pairs
_COMBINED_FIELD_MAP = {}
for _old in OLD_EXPLORE:
    for _new in (NEW_EXPLORE, NEW_EXPLORE_2):
        _COMBINED_FIELD_MAP.update(FIELD_MAPS.get((_old, _new), {}))


def check(sdk, source_id):
    print(f"\n=== Checking source dashboard {source_id} against {NEW_MODEL}/{NEW_EXPLORE} + {NEW_EXPLORE_2} ===\n")

    dest_fields = set()
    dest_views  = set()
    for explore_name in (NEW_EXPLORE, NEW_EXPLORE_2):
        try:
            exp = sdk.lookml_model_explore(NEW_MODEL, explore_name, fields="fields,joins")
        except Exception as e:
            print(f"❌ Could not load explore {NEW_MODEL}/{explore_name}: {e}")
            sys.exit(1)
        for f in (exp.fields.dimensions or []):
            dest_fields.add(f.name)
        for f in (exp.fields.measures or []):
            dest_fields.add(f.name)
        dest_views.update(f.split(".")[0] for f in dest_fields)
        if exp.joins:
            for j in exp.joins:
                if j.name:
                    dest_views.add(j.name)

    elements = sdk.dashboard_dashboard_elements(source_id)

    # Deduped summary buckets
    summary_bad           = {}   # old_field -> dest_field
    summary_missing_join  = set()
    summary_missing_field = set()
    summary_needs_mapping = set()

    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        if q.view not in OLD_EXPLORE:
            continue
        tile_title = el.title or "(untitled)"

        fields = set()
        for f in (q.fields or []):
            if "." in f and not f.startswith("__"):
                fields.add(f)
        for f in (q.filters or {}).keys():
            if "." in f and not f.startswith("__"):
                fields.add(f)
        for s in (q.sorts or []):
            f = s.split(" ")[0]
            if "." in f and not f.startswith("__"):
                fields.add(f)
        if q.dynamic_fields:
            try:
                for d in json.loads(q.dynamic_fields):
                    f = d.get("based_on", "")
                    if f and "." in f and not f.startswith("__"):
                        fields.add(f)
            except Exception:
                pass

        if not fields:
            continue

        ok, mapped, bad = [], [], []
        missing_join, missing_field, needs_mapping = [], [], []

        for f in sorted(fields):
            if f in dest_fields:
                ok.append(f)
            elif f in _COMBINED_FIELD_MAP:
                dest = _COMBINED_FIELD_MAP[f]
                if dest in dest_fields:
                    mapped.append((f, dest))
                else:
                    bad.append((f, dest))
                    summary_bad[f] = dest
            else:
                view = f.split(".")[0]
                if view not in dest_views:
                    missing_join.append(f)
                    summary_missing_join.add(f)
                else:
                    missing_field.append(f)
                    summary_missing_field.add(f)

        print(f"  Tile: '{tile_title}'")
        for f in ok:
            print(f"    ✅ {f}")
        for f, dest in mapped:
            print(f"    ✅ {f} → {dest}")
        for f, dest in bad:
            print(f"    ❌ {f} → {dest}  (FIELD_MAP destination not in explore)")
        for f in missing_join:
            print(f"    🔴 {f}  (view not joined into explore)")
        for f in missing_field:
            print(f"    🟡 {f}  (view is joined but field doesn't exist in LookML)")
        for f in needs_mapping:
            print(f"    ⚠️  {f}  (exists in explore but not in FIELD_MAP)")
        print()

    any_issues = summary_bad or summary_missing_join or summary_missing_field or summary_needs_mapping
    print("=== Summary ===")
    if not any_issues:
        print("✅ All fields accounted for.")
    else:
        if summary_bad:
            print("❌ Bad mappings (FIELD_MAP destination missing from explore):")
            for old, dest in sorted(summary_bad.items()):
                print(f"   {old} → {dest}")
        if summary_missing_join:
            print("🔴 Missing joins (view not joined into explore — fix in LookML explore definition):")
            for f in sorted(summary_missing_join):
                print(f"   {f}")
        if summary_missing_field:
            print("🟡 Missing fields (view is joined but dimension/measure needs to be written in LookML):")
            for f in sorted(summary_missing_field):
                print(f"   {f}")
        if summary_needs_mapping:
            print("⚠️  Needs mapping (field exists in explore but is missing from FIELD_MAP):")
            for f in sorted(summary_needs_mapping):
                print(f"   {f}")

    return not any_issues


def batch_check(sdk, entries):
    missing = defaultdict(lambda: {"new_field": None, "dashboards": defaultdict(set)})
    statuses = {}

    print(f"\n=== Batch Pre-Migration Check: {len(entries)} dashboards ===\n")

    # Load explore fields once (both explores)
    all_explore_fields = set()
    for _explore_name in (NEW_EXPLORE, NEW_EXPLORE_2):
        try:
            _exp = sdk.lookml_model_explore(NEW_MODEL, _explore_name, fields="fields")
            for f in (_exp.fields.dimensions or []):
                all_explore_fields.add(f.name)
            for f in (_exp.fields.measures or []):
                all_explore_fields.add(f.name)
        except Exception as e:
            print(f"❌ Could not load explore {NEW_MODEL}/{_explore_name}: {e}")
            sys.exit(1)

    for entry in entries:
        src, dst = entry.split(":", 1) if ":" in entry else (entry, None)
        label = f"{src} -> {dst}" if dst else src
        print(f"Checking {label}...", end=" ", flush=True)

        try:
            elements = sdk.dashboard_dashboard_elements(src)
        except Exception as e:
            print(f"❌ could not fetch: {e}")
            statuses[label] = "❌"
            continue

        dashboard_issues = False
        for el in elements:
            if not el.query_id:
                continue
            q = sdk.query(str(el.query_id))
            # Skip tiles not on the old explore
            if q.view not in OLD_EXPLORE:
                continue
            el_fields = set(q.fields or []) | set((q.filters or {}).keys())
            # Collect based_on fields from dynamic fields
            based_on_fields = set()
            if q.dynamic_fields:
                try:
                    for d in json.loads(q.dynamic_fields):
                        if d.get("based_on"):
                            based_on_fields.add(d["based_on"])
                except Exception:
                    pass
            tile = el.title or "(untitled)"
            # Only real LookML fields, skip table calc names like __calc__
            lookml_fields = {f for f in el_fields if "." in f and not f.startswith("__")}
            lookml_fields |= {f for f in based_on_fields if "." in f and not f.startswith("__")}
            for f in lookml_fields:
                new_field = _COMBINED_FIELD_MAP.get(f)
                if new_field:
                    # Field is mapped — check the destination exists in new explore
                    if new_field not in all_explore_fields:
                        missing[f]["new_field"] = new_field
                        missing[f]["dashboards"][label].add(tile)
                        dashboard_issues = True
                elif f not in all_explore_fields:
                    # Field is not mapped and not in new explore — genuinely missing
                    missing[f]["new_field"] = None
                    missing[f]["dashboards"][label].add(tile)
                    dashboard_issues = True
                # else: field exists in new explore already, no action needed
        statuses[label] = "⚠️" if dashboard_issues else "✅"
        print(statuses[label])

    print()
    if missing:
        print("=== Missing Fields ===")
        for i, (old_field, info) in enumerate(missing.items(), 1):
            new_field = info["new_field"]
            if new_field:
                print(f"{i}. {old_field} -> ❌ {new_field} (missing in new explore)")
            else:
                print(f"{i}. {old_field} — not in FIELD_MAP")
            for dash, tiles in info["dashboards"].items():
                tile_list = ", ".join(f"'{t}'" for t in sorted(tiles))
                print(f"   dashboard {dash}: {tile_list}")
    else:
        print("✅ All dashboards clean — safe to migrate")

    print()
    ready = sum(1 for s in statuses.values() if s == "✅")
    needs = sum(1 for s in statuses.values() if s == "⚠️")
    print("=== Summary ===")
    if ready:
        print(f"✅ {ready} dashboard(s) ready to migrate")
    if needs:
        print(f"⚠️  {needs} dashboard(s) need attention — fix fields above then re-run")
