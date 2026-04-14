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
from checks import check, batch_check


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

def remap_fields(fields, field_map, tile_title=None):
    if not fields:
        return fields
    result = []
    for f in fields:
        if is_problem_field(f, field_map):
            print(f"  ⚠️  WILL BREAK '{tile_title}' — field not available in new explore: {f}")
        result.append(field_map.get(f, f))
    return result

def remap_filters(filters, field_map, tile_title=None):
    if not filters:
        return filters
    result = {}
    for k, v in filters.items():
        if is_problem_field(k, field_map):
            print(f"  ⚠️  WILL BREAK '{tile_title}' — filter not available in new explore: {k}")
        result[field_map.get(k, k)] = v
    return result

def remap_sorts(sorts, field_map):
    if not sorts:
        return sorts
    new_sorts = []
    for sort in sorts:
        for old, new in field_map.items():
            if old in sort:
                sort = sort.replace(old, new)
        new_sorts.append(sort)
    return new_sorts

def remap_vis_config(vc, field_map):
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
            vc[key] = {field_map.get(k, k): v for k, v in vc[key].items()}

    # Keys whose values are lists of field names
    list_keyed = ("hidden_fields", "column_order", "hidden_pivots")
    for key in list_keyed:
        if vc.get(key) and isinstance(vc[key], list):
            vc[key] = [field_map.get(f, f) for f in vc[key]]

    return vc


def remap_dynamic_fields(dynamic_fields_str, field_map):
    if not dynamic_fields_str:
        return dynamic_fields_str
    customs = json.loads(dynamic_fields_str)
    for c in customs:
        if c.get("based_on") in field_map:
            c["based_on"] = field_map[c["based_on"]]
        if c.get("filters"):
            c["filters"] = {field_map.get(k, k): v for k, v in c["filters"].items()}
        if c.get("expression"):
            for old, new in field_map.items():
                c["expression"] = c["expression"].replace("${" + old + "}", "${" + new + "}")
        if c.get("filter_expression"):
            for old, new in field_map.items():
                c["filter_expression"] = c["filter_expression"].replace("${" + old + "}", "${" + new + "}")
        if c.get("args"):
            c["args"] = [field_map.get(a, a) if isinstance(a, str) else a for a in c["args"]]
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

def is_problem_field(field, field_map):
    """Returns True if a field needs to be flagged — it's from OLD_EXPLORE and unmapped,
    or from a view that isn't in the new explore (checked via API if available)."""
    if not field or "." not in field:
        return False
    view = field.split(".")[0]
    if field in field_map:
        return False  # explicitly remapped, fine
    if view == OLD_EXPLORE:
        return True   # from old explore and not remapped
    # Use API-loaded explore fields if available
    if _EXPLORE_VIEWS:
        return view not in _EXPLORE_VIEWS
    # Fallback to hardcoded set
    return view not in JOINED_VIEWS_IN_NEW_EXPLORE





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
            # Route on source fields first so we can select the right field map
            target_explore = route_explore(
                list(q.fields or []) + list((q.filters or {}).keys()),
                _EXCLUSIVE_1, _EXCLUSIVE_2,
            )
            field_map = FIELD_MAPS.get((OLD_EXPLORE, target_explore), FIELD_MAP)
            remapped_fields  = remap_fields(q.fields, field_map, el.title)
            remapped_filters = remap_filters(q.filters, field_map, el.title)
            new_query = sdk.create_query(models.WriteQuery(
                model=NEW_MODEL,
                view=target_explore,
                fields=remapped_fields,
                filters=remapped_filters,
                sorts=remap_sorts(q.sorts, field_map),
                limit=q.limit,
                dynamic_fields=remap_dynamic_fields(q.dynamic_fields, field_map),
                pivots=remap_fields(q.pivots, field_map),
                vis_config=remap_vis_config(vc, field_map),
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
                            field=field_map.get(l.field, l.field),
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
        if f.dimension and is_problem_field(f.dimension, FIELD_MAP):
            issues.append(f"Dashboard filter '{f.title}': {f.dimension}")

    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        field_map = FIELD_MAPS.get((OLD_EXPLORE, q.view), FIELD_MAP)
        if q.view == OLD_EXPLORE:
            issues.append(f"Tile '{el.title}' still on old explore")
        if q.filters:
            for field in q.filters:
                if is_problem_field(field, field_map):
                    issues.append(f"Tile '{el.title}' filter: {field}")
        if q.sorts:
            for sort in q.sorts:
                field = sort.split(" ")[0]
                if is_problem_field(field, field_map):
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

    if not args.production:
        sdk.update_session(models.WriteApiSession(workspace_id="dev"))
    try:
        sdk.update_git_branch(project_id=NEW_MODEL, body=models.WriteGitBranch(name="v2-migration"))
    except Exception as e:
        print(f"⚠️  Could not switch to v2-migration branch (proceeding on current branch): {e}")

    if args.check or args.audit or args.validate or args.check_explore:
        ok = check(sdk, args.source)
        sys.exit(0 if ok else 1)

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
        batch_check(sdk, args.batch)
        sys.exit(0)

    source_id = args.source
    dest_id   = args.dest

    print(f"\nMigrating dashboard {source_id} → {dest_id}")

    snapshot(sdk, dest_id)
    delete_tiles_and_copy_source_dashboard(sdk, source_id, dest_id)
    verify(sdk, dest_id)
    print(f"\n✓ Done — snapshot saved to snapshot_{dest_id}.json")
