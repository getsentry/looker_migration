"""
Looker Dashboard Migration Script
==================================
Migrates dashboard tiles from an old explore to a new one,
copying vis_config, totals, and fixing filters.

USAGE:
  python3 run_migration.py --source 1722 --dest 2137 --check
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
from mappings import OLD_EXPLORE, NEW_MODEL, JOINED_VIEWS_IN_NEW_EXPLORE, FIELD_MAPS, FIELD_MAP
from checks import check, batch_check  # check used by --check / --batch paths


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Looker dashboard migration tool")
    p.add_argument("--source",        required=False, default=None, help="Source dashboard ID (copy FROM)")
    p.add_argument("--batch",         nargs="+", metavar="ID", help="Validate multiple source dashboard IDs (or SOURCE:DEST pairs)")
    p.add_argument("--dest",          required=False, default=None, help="Destination dashboard ID (copy TO)")
    p.add_argument("--check",         action="store_true", help="Check source dashboard fields against the destination explore (API-based, grouped by tile)")
    p.add_argument("--ini",           default="looker.ini", help="Path to looker.ini (default: ./looker.ini)")
    p.add_argument("--production",    action="store_true", help="Run against production (skip dev session and git branch switch)")
    p.add_argument("--explore-from",  nargs="+", default=["product_facts"], metavar="EXPLORE", help="Old explore name(s) (default: product_facts)")
    p.add_argument("--explore-to",    default="product_usage_org_proj", help="New explore name (default: product_usage_org_proj)")
    p.add_argument("--model",          default="super_big_facts", help="New model name (default: super_big_facts)")
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

def remap_fields(fields, field_map):
    if not fields:
        return fields
    return [field_map.get(f, f) for f in fields]

def remap_filters(filters, field_map):
    if not filters:
        return filters
    return {field_map.get(k, k): v for k, v in filters.items()}

def broken_fields(fields, filters, dynamic_fields_str, field_map, target_explore=None):
    """Return list of fields/filter-keys that won't exist in the target explore."""
    candidates = list(fields or []) + list((filters or {}).keys())
    if dynamic_fields_str:
        for c in json.loads(dynamic_fields_str):
            if c.get("based_on"):
                candidates.append(c["based_on"])
            candidates.extend((c.get("filters") or {}).keys())
            for expr_key in ("expression", "filter_expression"):
                if c.get(expr_key):
                    candidates += re.findall(r'\$\{([^}]+)\}', c[expr_key])
    return [f for f in candidates if is_problem_field(f, field_map, target_explore)]

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
_EXPLORE_VIEW_SETS = {}   # explore_name -> set of view names
_EXPLORE_FIELD_SETS = {}  # explore_name -> set of field names (view.field)


def build_explore_view_sets(sdk):
    """Fetch view and field sets for all new explores referenced in FIELD_MAPS.

    Returns (view_sets, field_sets) where each maps explore_name -> set.
    """
    new_explores = {new_exp for (_, new_exp) in FIELD_MAPS}
    view_sets = {}
    field_sets = {}
    for explore_name in new_explores:
        exp = sdk.lookml_model_explore(NEW_MODEL, explore_name, fields="fields")
        fields = (exp.fields.dimensions or []) + (exp.fields.measures or [])
        view_sets[explore_name] = {f.name.split(".")[0] for f in fields}
        field_sets[explore_name] = {f.name for f in fields}
    return view_sets, field_sets


def route_explore(source_explore, fields, explore_view_sets):
    """Pick the right new explore for a tile given its source explore and fields.

    Derives candidate new explores from FIELD_MAPS. If only one candidate exists,
    returns it immediately. Otherwise scans fields for the first view that appears
    exclusively in one candidate explore.
    """
    candidates = [new_exp for (old, new_exp) in FIELD_MAPS if old == source_explore]
    if not candidates:
        raise ValueError(f"No FIELD_MAPS entry for source explore: {source_explore}")
    if len(candidates) == 1:
        return candidates[0]
    for f in (fields or []):
        if "." not in f:
            continue
        # If the field has a mapping in exactly one candidate's map, route there.
        # This handles renamed views (e.g. data_by_sdk → sdk_base_events) where the
        # source view name won't appear in the destination explore's view set.
        exclusive = [c for c in candidates if f in FIELD_MAPS.get((source_explore, c), {})]
        if len(exclusive) == 1:
            return exclusive[0]
    for f in (fields or []):
        if "." not in f:
            continue
        view = f.split(".")[0]
        for candidate in candidates:
            other_views = set().union(*(explore_view_sets.get(c, set()) for c in candidates if c != candidate))
            if view in explore_view_sets.get(candidate, set()) and view not in other_views:
                return candidate
    return candidates[0]

def is_problem_field(field, field_map, target_explore=None):
    """Returns True if a field will be unavailable in the target explore after remapping."""
    if not field or "." not in field:
        return False
    if field in field_map:
        return False  # explicitly remapped, fine
    # Check against the specific routed explore's field set if available
    if target_explore and _EXPLORE_FIELD_SETS:
        return field not in _EXPLORE_FIELD_SETS.get(target_explore, set())
    # Fall back to union of all explore views
    if _EXPLORE_VIEWS:
        return field.split(".")[0] not in _EXPLORE_VIEWS
    return field.split(".")[0] not in JOINED_VIEWS_IN_NEW_EXPLORE





# ─────────────────────────────────────────────
# Delete tiles and copy source dashboard
# ─────────────────────────────────────────────
def delete_tiles_and_copy_source_dashboard(sdk, source_id, dest_id):
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

    # Build source position map and get dest layout ID before the loop
    src_layout   = next((l for l in sdk.dashboard_dashboard_layouts(str(source_id)) if getattr(l, "active", False)), None)
    src_pos      = {c.dashboard_element_id: c for c in (src_layout.dashboard_layout_components or [])} if src_layout else {}
    dst_layout_id = next((str(l.id) for l in sdk.dashboard_dashboard_layouts(str(dest_id)) if getattr(l, "active", False)), None)

    # ── 3. Recreate elements (with remapping), link filters, restore positions ─
    broken_summary = {}  # tile_title -> [broken_field, ...]
    src_to_dest = {}     # source element_id -> dest element_id (for layout update)
    for el in source_elements:
        field_map = {}  # default: no remapping (used by filterable section below)
        if el.query_id:
            q = sdk.query(str(el.query_id))
            vc, _ = extract_vis_config(el, q)
            if q.view in OLD_EXPLORE:
                # Route on source fields first so we can select the right field map
                target_explore = route_explore(
                    q.view,
                    list(q.fields or []) + list((q.filters or {}).keys()),
                    _EXPLORE_VIEW_SETS,
                )
                field_map = FIELD_MAPS.get((q.view, target_explore), FIELD_MAP)
                new_query = sdk.create_query(models.WriteQuery(
                    model=NEW_MODEL,
                    view=target_explore,
                    fields=remap_fields(q.fields, field_map),
                    filters=remap_filters(q.filters, field_map),
                    sorts=remap_sorts(q.sorts, field_map),
                    limit=q.limit,
                    dynamic_fields=remap_dynamic_fields(q.dynamic_fields, field_map),
                    pivots=remap_fields(q.pivots, field_map),
                    vis_config=remap_vis_config(vc, field_map),
                    total=q.total,
                    row_total=q.row_total,
                    filter_config=None,
                ))
                bad = broken_fields(new_query.fields, new_query.filters, new_query.dynamic_fields, field_map, new_query.view)
                if bad:
                    broken_summary[el.title or "(untitled)"] = bad
                query_id = new_query.id
            else:
                query_id = el.query_id
        else:
            query_id = None

        new_el = sdk.create_dashboard_element(models.WriteDashboardElement(
            dashboard_id=str(dest_id),
            query_id=query_id,
            look_id=el.look_id if query_id is None else None,
            result_maker_id=el.result_maker_id if query_id is None else None,
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
            src_to_dest[el.id] = new_el.id

    # Update all positions in a single layout fetch
    if dst_layout_id and src_to_dest:
        dest_comps = {x.dashboard_element_id: x for x in (sdk.dashboard_layout(dst_layout_id).dashboard_layout_components or [])}
        for src_el_id, dest_el_id in src_to_dest.items():
            if dest_el_id in dest_comps:
                c = src_pos[src_el_id]
                sdk.update_dashboard_layout_component(str(dest_comps[dest_el_id].id),
                    models.WriteDashboardLayoutComponent(
                        dashboard_layout_id=dst_layout_id,
                        dashboard_element_id=str(dest_el_id),
                        row=c.row,
                        column=c.column,
                        width=c.width,
                        height=c.height,
                    ))

    query_tile_count = sum(1 for el in source_elements if el.query_id)
    print(f"✓ Rebuilt dashboard {dest_id} ({len(source_elements)} tiles, {query_tile_count} with queries)")
    if broken_summary:
        print(f"\n⚠️  {len(broken_summary)} tile(s) with unmapped fields:")
        for title, fields in broken_summary.items():
            print(f"  '{title}':")
            for f in fields:
                print(f"    {f}")
    if query_tile_count:
        clean = query_tile_count - len(broken_summary)
        pct = 100 * clean // query_tile_count
        print(f"\n  {clean}/{query_tile_count} tiles migrated cleanly ({pct}%)")




# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    sdk = looker_sdk.init40(config_file=args.ini)

    OLD_EXPLORE = args.explore_from  # list of old explore names
    NEW_MODEL   = args.model
    NEW_EXPLORE = args.explore_to

    if not args.production:
        sdk.update_session(models.WriteApiSession(workspace_id="dev"))
    try:
        sdk.update_git_branch(project_id=NEW_MODEL, body=models.WriteGitBranch(name="v2-migration"))
    except Exception as e:
        print(f"⚠️  Could not switch to v2-migration branch (proceeding on current branch): {e}")

    if args.check:
        ok = check(sdk, args.source)
        sys.exit(0 if ok else 1)

    # Load explore view sets for routing and is_problem_field
    try:
        _view_sets, _field_sets = build_explore_view_sets(sdk)
        _EXPLORE_VIEW_SETS.update(_view_sets)
        _EXPLORE_FIELD_SETS.update(_field_sets)
        _EXPLORE_VIEWS.update(set().union(*_view_sets.values()))
    except Exception as _e:
        print(f"⚠️  Could not load explore fields: {_e}")

    # --batch: validate multiple dashboards, deduped missing fields
    if args.batch:
        batch_check(sdk, args.batch)
        sys.exit(0)

    source_id = args.source
    dest_id   = args.dest

    print(f"\nMigrating dashboard {source_id} → {dest_id}")

    delete_tiles_and_copy_source_dashboard(sdk, source_id, dest_id)
    print(f"\n✓ Done")
