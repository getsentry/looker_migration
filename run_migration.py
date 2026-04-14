"""
Looker Dashboard Migration Script
==================================
Migrates dashboard tiles from an old explore to a new one,
copying vis_config, totals, and fixing filters.

USAGE:
  python3 run_migration.py --source 1722 --dest 2137 --dry-run
  python3 run_migration.py --source 1722 --dest 2137 --validate
  python3 run_migration.py --source 1722 --dest 2137

CREDENTIALS:
  Set in looker.ini:
    [Looker]
    base_url=https://your-instance.cloud.looker.com
    client_id=your_client_id
    client_secret=your_client_secret

  Or pass a custom path:
    python3 run_migration.py --source 1722 --dest 2137 --ini ~/my_looker.ini
"""

import argparse
import json
import re
import sys
import looker_sdk
from looker_sdk import models40 as models
from mappings import OLD_EXPLORE, NEW_MODEL, NEW_EXPLORE, NEW_EXPLORE_2, JOINED_VIEWS_IN_NEW_EXPLORE, FIELD_MAPS, FIELD_MAP


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Looker dashboard migration tool")
    p.add_argument("--source",        required=False, default=None, help="Source dashboard ID (copy FROM)")
    p.add_argument("--batch",         nargs="+", metavar="ID", help="Validate multiple source dashboard IDs (or SOURCE:DEST pairs)")
    p.add_argument("--dest",          required=False, default=None, help="Destination dashboard ID (copy TO)")
    p.add_argument("--check",         action="store_true", help="Check source dashboard fields against the destination explore (API-based, grouped by tile)")
    p.add_argument("--check-tiles",   action="store_true", help="Check source dashboard fields tile-by-tile, mapped fields first, including dynamic field expressions")
    p.add_argument("--validate",      action="store_true", help="[deprecated] Alias for --check")
    p.add_argument("--check-explore", action="store_true", help="[deprecated] Alias for --check")
    p.add_argument("--audit",         action="store_true", help="[deprecated] Alias for --check")
    p.add_argument("--ini",           default="looker.ini", help="Path to looker.ini (default: ./looker.ini)")
    p.add_argument("--production",    action="store_true", help="Run against production (skip dev session and git branch switch)")
    p.add_argument("--explore-from",  default="product_facts", help="Old explore name (default: product_facts)")
    p.add_argument("--explore-to",    default="product_usage_org_proj", help="New explore name (default: product_usage_org_proj)")
    p.add_argument("--model",         default="super_big_facts", help="New model name (default: super_big_facts)")
    return p.parse_args()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def extract_vis_config(element, query=None):
    vc = getattr(element, "vis_config", None)
    if vc and isinstance(vc, dict) and vc.get("type"):
        return vc, "element.vis_config"
    rm = getattr(element, "result_maker", None)
    if rm:
        vc = getattr(rm, "vis_config", None)
        if vc and isinstance(vc, dict) and vc.get("type"):
            return vc, "result_maker.vis_config"
    if query:
        vc = getattr(query, "vis_config", None)
        if vc and isinstance(vc, dict) and vc.get("type"):
            return vc, "query.vis_config"
    return None, "not found"

def remap_fields(fields, tile_title=None):
    if not fields:
        return fields
    result = []
    for f in fields:
        if is_problem_field(f):
            print(f"  ⚠️  WILL BREAK '{tile_title}' — field not available in new explore: {f}")
        result.append(FIELD_MAP.get(f, f))
    return result

def remap_filters(filters, tile_title=None):
    if not filters:
        return filters
    result = {}
    for k, v in filters.items():
        if is_problem_field(k):
            print(f"  ⚠️  WILL BREAK '{tile_title}' — filter not available in new explore: {k}")
        result[FIELD_MAP.get(k, k)] = v
    return result

def remap_sorts(sorts):
    if not sorts:
        return sorts
    new_sorts = []
    for sort in sorts:
        for old, new in FIELD_MAP.items():
            if old in sort:
                sort = sort.replace(old, new)
        new_sorts.append(sort)
    return new_sorts

def remap_vis_config(vc):
    """Remap field names inside vis_config for keys that reference LookML fields."""
    if not vc:
        return vc
    vc = dict(vc)  # shallow copy — don't mutate the source

    # Keys whose values are dicts keyed by field name
    dict_keyed = (
        "series_colors", "series_labels", "series_types", "series_point_styles",
        "series_collapsed", "series_cell_visualizations", "series_value_format",
        "series_text_format", "series_sizing", "series_axis_id", "series_error_type",
    )
    for key in dict_keyed:
        if vc.get(key) and isinstance(vc[key], dict):
            vc[key] = {FIELD_MAP.get(k, k): v for k, v in vc[key].items()}

    # Keys whose values are lists of field names
    list_keyed = ("hidden_fields", "column_order", "hidden_pivots")
    for key in list_keyed:
        if vc.get(key) and isinstance(vc[key], list):
            vc[key] = [FIELD_MAP.get(f, f) for f in vc[key]]

    return vc


def remap_dynamic_fields(dynamic_fields_str):
    if not dynamic_fields_str:
        return dynamic_fields_str
    customs = json.loads(dynamic_fields_str)
    for c in customs:
        if c.get("based_on") in FIELD_MAP:
            c["based_on"] = FIELD_MAP[c["based_on"]]
        if c.get("filters"):
            c["filters"] = {FIELD_MAP.get(k, k): v for k, v in c["filters"].items()}
        if c.get("expression"):
            for old, new in FIELD_MAP.items():
                c["expression"] = c["expression"].replace("${" + old + "}", "${" + new + "}")
        if c.get("filter_expression"):
            for old, new in FIELD_MAP.items():
                c["filter_expression"] = c["filter_expression"].replace("${" + old + "}", "${" + new + "}")
        if c.get("args"):
            c["args"] = [FIELD_MAP.get(a, a) if isinstance(a, str) else a for a in c["args"]]
    return json.dumps(customs)

# Populated at runtime after SDK is initialized and dev mode is set
_EXPLORE_VIEWS = set()
_EXCLUSIVE_1 = set()   # views only in NEW_EXPLORE
_EXCLUSIVE_2 = set()   # views only in NEW_EXPLORE_2


def build_explore_view_sets(sdk):
    """Fetch both explores from the API and return their view sets.

    Returns:
        (views1, views2, exclusive1, exclusive2) where exclusive1/exclusive2
        are the views that appear in only one explore.
    """
    def _views(exp):
        fields = (exp.fields.dimensions or []) + (exp.fields.measures or [])
        return {f.name.split(".")[0] for f in fields}

    exp1 = sdk.lookml_model_explore(NEW_MODEL, NEW_EXPLORE, fields="fields")
    exp2 = sdk.lookml_model_explore(NEW_MODEL, NEW_EXPLORE_2, fields="fields")
    views1 = _views(exp1)
    views2 = _views(exp2)
    return views1, views2, views1 - views2, views2 - views1


def route_explore(fields, exclusive1, exclusive2):
    """Pick the right explore for a tile based on its fields.

    Scans fields for the first view that appears exclusively in one explore.
    Falls back to NEW_EXPLORE if no exclusive view is found, and logs a warning
    since those tiles reference only shared views — the fallback may be wrong.
    """
    for f in (fields or []):
        if "." not in f:
            continue
        view = f.split(".")[0]
        if view in exclusive2:
            print(f"  {NEW_EXPLORE_2} selected")
            return NEW_EXPLORE_2
        if view in exclusive1:
            print(f"  {NEW_EXPLORE} selected")
            return NEW_EXPLORE
    print(f"  defaulting to {NEW_EXPLORE}")
    return NEW_EXPLORE

def is_problem_field(field):
    """Returns True if a field needs to be flagged — it's from OLD_EXPLORE and unmapped,
    or from a view that isn't in the new explore (checked via API if available)."""
    if not field or "." not in field:
        return False
    view = field.split(".")[0]
    if field in FIELD_MAP:
        return False  # explicitly remapped, fine
    if view == OLD_EXPLORE:
        return True   # from old explore and not remapped
    # Use API-loaded explore fields if available
    if _EXPLORE_VIEWS:
        return view not in _EXPLORE_VIEWS
    # Fallback to hardcoded set
    return view not in JOINED_VIEWS_IN_NEW_EXPLORE





# ─────────────────────────────────────────────
# CHECK
# ─────────────────────────────────────────────
def check(sdk, source_id):
    print(f"\n=== Checking source dashboard {source_id} against {NEW_MODEL}/{NEW_EXPLORE} ===\n")

    try:
        exp = sdk.lookml_model_explore(NEW_MODEL, NEW_EXPLORE, fields="fields,joins")
    except Exception as e:
        print(f"❌ Could not load explore {NEW_MODEL}/{NEW_EXPLORE}: {e}")
        sys.exit(1)

    dest_fields = set()
    for f in (exp.fields.dimensions or []):
        dest_fields.add(f.name)
    for f in (exp.fields.measures or []):
        dest_fields.add(f.name)

    # Views that are actually joined into the explore
    dest_views = {f.split(".")[0] for f in dest_fields}
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
            elif f in FIELD_MAP:
                dest = FIELD_MAP[f]
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

# ─────────────────────────────────────────────
# STEP 1: Snapshot
# ─────────────────────────────────────────────
def snapshot(sdk, dest_id):
    print(f"\n=== Step 1: Snapshot dashboard {dest_id} ===")
    elements = sdk.dashboard_dashboard_elements(dest_id)
    snapshot_data = []
    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        vc, loc = extract_vis_config(el, q)
        if not vc:
            print(f"  ⚠️  '{el.title}' — vis_config not found")
        else:
            print(f"  ✅ '{el.title}' — {vc.get('type')} at {loc}")
        snapshot_data.append({
            "element_id": el.id,
            "title": el.title,
            "query_id": el.query_id,
            "result_maker_id": el.result_maker_id,
            "model": q.model,
            "view": q.view,
            "fields": q.fields,
            "filters": q.filters,
            "sorts": q.sorts,
            "limit": q.limit,
            "dynamic_fields": q.dynamic_fields,
            "vis_config": vc,
            "vis_config_source": loc,
        })
    fname = f"snapshot_{dest_id}.json"
    with open(fname, "w") as f:
        json.dump(snapshot_data, f, indent=2, default=str)
    print(f"✓ Snapshot saved to {fname} ({len(snapshot_data)} tiles)")


# ─────────────────────────────────────────────
# STEP 2: Delete tiles and copy source dashboard
# ─────────────────────────────────────────────
def delete_tiles_and_copy_source_dashboard(sdk, source_id, dest_id):
    print(f"\n=== Step 2: Delete dest tiles and copy from source {source_id} ===")

    dest_elements    = sdk.dashboard_dashboard_elements(dest_id)
    source_elements  = sdk.dashboard_dashboard_elements(source_id)
    source_dashboard = sdk.dashboard(source_id)

    # ── 1. Delete everything on the dest dashboard ───────────────────────────
    dest_dashboard = sdk.dashboard(dest_id)
    for f in (dest_dashboard.dashboard_filters or []):
        sdk.delete_dashboard_filter(str(f.id))
    for el in dest_elements:
        sdk.delete_dashboard_element(str(el.id))

    # ── 2. Recreate dashboard-level filters from source ───────────────────────
    for f in (source_dashboard.dashboard_filters or []):
        sdk.create_dashboard_filter(
            models.WriteCreateDashboardFilter(
                dashboard_id=str(dest_id),
                name=f.name,
                title=f.title,
                type=f.type,
                default_value=f.default_value,
                model=f.model,
                explore=f.explore,
                dimension=FIELD_MAP.get(f.dimension, f.dimension),
                row=f.row,
                listens_to_filters=f.listens_to_filters,
                allow_multiple_values=f.allow_multiple_values,
                required=f.required,
                ui_config=f.ui_config,
            )
        )
        print(f"  filter: {f.title}")

    # Build source position map and get dest layout ID before the loop
    src_layout   = next((l for l in sdk.dashboard_dashboard_layouts(str(source_id)) if getattr(l, "active", False)), None)
    src_pos      = {c.dashboard_element_id: c for c in (src_layout.dashboard_layout_components or [])} if src_layout else {}
    dst_layout_id = next((str(l.id) for l in sdk.dashboard_dashboard_layouts(str(dest_id)) if getattr(l, "active", False)), None)

    # ── 3. Recreate elements (with remapping), link filters, restore positions ─
    for el in source_elements:
        if el.query_id:
            q = sdk.query(str(el.query_id))
            vc, _ = extract_vis_config(el, q)
            remapped_fields  = remap_fields(q.fields, el.title)
            remapped_filters = remap_filters(q.filters, el.title)
            target_explore   = route_explore(
                list(remapped_fields or []) + list((remapped_filters or {}).keys()),
                _EXCLUSIVE_1, _EXCLUSIVE_2,
            )
            new_query = sdk.create_query(models.WriteQuery(
                model=NEW_MODEL,
                view=target_explore,
                fields=remapped_fields,
                filters=remapped_filters,
                sorts=remap_sorts(q.sorts),
                limit=q.limit,
                dynamic_fields=remap_dynamic_fields(q.dynamic_fields),
                pivots=remap_fields(q.pivots),
                vis_config=remap_vis_config(vc),
                total=q.total,
                row_total=q.row_total,
                filter_config=None,
            ))
            query_id = new_query.id
        else:
            query_id = None

        new_el = sdk.create_dashboard_element(models.WriteDashboardElement(
            dashboard_id=str(dest_id),
            query_id=query_id,
            title=el.title,
            title_hidden=el.title_hidden,
            subtitle_text=el.subtitle_text,
            body_text=el.body_text,
            note_text=el.note_text,
            note_display=el.note_display,
            note_state=el.note_state,
            type=el.type,
            rich_content_json=el.rich_content_json,
        ))
        print(f"  tile: {el.title}")

        if el.result_maker and el.result_maker.filterables:
            remapped_filterables = [
                models.ResultMakerFilterables(
                    model=f.model, view=f.view, name=f.name,
                    listen=[
                        models.ResultMakerFilterablesListen(
                            dashboard_filter_name=l.dashboard_filter_name,
                            field=FIELD_MAP.get(l.field, l.field),
                        ) for l in (f.listen or [])
                    ]
                ) for f in el.result_maker.filterables
            ]
            sdk.update_dashboard_element(str(new_el.id), models.WriteDashboardElement(
                result_maker=models.WriteResultMakerWithIdVisConfigAndDynamicFields(
                    filterables=remapped_filterables)
            ))

        if dst_layout_id and el.id in src_pos:
            c = src_pos[el.id]
            fresh_comps = {x.dashboard_element_id: x for x in (sdk.dashboard_layout(dst_layout_id).dashboard_layout_components or [])}
            if new_el.id in fresh_comps:
                sdk.update_dashboard_layout_component(str(fresh_comps[new_el.id].id),
                    models.WriteDashboardLayoutComponent(
                        dashboard_layout_id=dst_layout_id,
                        dashboard_element_id=str(new_el.id),
                        row=c.row,
                        column=c.column,
                        width=c.width,
                        height=c.height,
                    ))




# ─────────────────────────────────────────────
# STEP 3: Verify
# ─────────────────────────────────────────────
def verify(sdk, dest_id):
    print("\n=== Step 3: Verify ===")
    elements = sdk.dashboard_dashboard_elements(dest_id)
    issues = []
    dashboard = sdk.dashboard(dest_id)

    for f in (dashboard.dashboard_filters or []):
        if f.dimension and is_problem_field(f.dimension):
            issues.append(f"Dashboard filter '{f.title}': {f.dimension}")

    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        if q.view == OLD_EXPLORE:
            issues.append(f"Tile '{el.title}' still on old explore")
        if q.filters:
            for field in q.filters:
                if is_problem_field(field):
                    issues.append(f"Tile '{el.title}' filter: {field}")
        if q.sorts:
            for sort in q.sorts:
                field = sort.split(" ")[0]
                if is_problem_field(field):
                    issues.append(f"Tile '{el.title}' sort: {sort}")
        vc, loc = extract_vis_config(el, q)
        if not vc:
            issues.append(f"Tile '{el.title}' missing vis_config — may show as Table (Legacy)")
        else:
            print(f"  ✅ '{el.title}' — {vc.get('type')} at {loc}")

    if issues:
        print("\n⚠️  Issues found:")
        for i in issues:
            print(f"  - {i}")
    else:
        print("\n✓ All clean")


# ─────────────────────────────────────────────
# ROLLBACK
# ─────────────────────────────────────────────
def rollback(sdk, dest_id):
    print(f"Rolling back dashboard {dest_id}...")
    fname = f"snapshot_{dest_id}.json"
    with open(fname) as f:
        snapshot_data = json.load(f)
    for tile in snapshot_data:
        sdk.update_dashboard_element(
            str(tile["element_id"]),
            models.WriteDashboardElement(query_id=tile["query_id"])
        )
        print(f"  ✅ Restored: {tile['title']}")
    print("Rollback complete")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    sdk = looker_sdk.init40(config_file=args.ini)

    OLD_EXPLORE = args.explore_from
    NEW_MODEL   = args.model
    NEW_EXPLORE = args.explore_to

    # --check runs against production — do it before switching to dev
    if args.check or args.audit or args.validate or args.check_explore:
        ok = check(sdk, args.source)
        sys.exit(0 if ok else 1)

    if not args.production:
        sdk.update_session(models.WriteApiSession(workspace_id="dev"))
    try:
        sdk.update_git_branch(project_id=NEW_MODEL, body=models.WriteGitBranch(name="v2-migration"))
    except Exception as e:
        print(f"⚠️  Could not switch to v2-migration branch (proceeding on current branch): {e}")

    # Load both explore view sets for routing and is_problem_field
    try:
        _views1, _views2, _excl1, _excl2 = build_explore_view_sets(sdk)
        _EXCLUSIVE_1.update(_excl1)
        _EXCLUSIVE_2.update(_excl2)
        _EXPLORE_VIEWS.update(_views1 | _views2)
    except Exception as _e:
        print(f"⚠️  Could not load explore fields: {_e}")

    # --batch: validate multiple dashboards, deduped missing fields
    if args.batch:
        from collections import defaultdict
        missing = defaultdict(lambda: {"new_field": None, "dashboards": defaultdict(set)})
        statuses = {}

        print(f"\n=== Batch Pre-Migration Check: {len(args.batch)} dashboards ===\n")

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

        for entry in args.batch:
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
                if q.model != NEW_MODEL or q.view != OLD_EXPLORE:
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
                    new_field = FIELD_MAP.get(f)
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
        sys.exit(0)

    source_id = args.source
    dest_id   = args.dest

    print(f"\nMigrating dashboard {source_id} → {dest_id}")

    snapshot(sdk, dest_id)
    delete_tiles_and_copy_source_dashboard(sdk, source_id, dest_id)
    verify(sdk, dest_id)
    print(f"\n✓ Done — snapshot saved to snapshot_{dest_id}.json")
