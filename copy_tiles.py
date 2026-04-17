"""
Looker Tile Copy Script
========================
Copies specific tiles from a source dashboard to a destination dashboard,
applying field remapping. The source dashboard is never modified.

USAGE:
  python3 copy_tiles.py --source 1261 --dest 2220 --tiles "Tile A" "Tile B"
  python3 copy_tiles.py --source 1261 --dest 2220 --tiles "Tile A" --dry-run
  python3 copy_tiles.py --source 1261 --dest 2220 --tiles "Tile A" --ini ~/my_looker.ini

NOTE:
  Tiles are added to the destination dashboard at their original positions
  from the source. If they overlap existing tiles on the destination, you
  can reposition them in the Looker UI afterward.

CREDENTIALS:
  Set in looker.ini:
    [Looker]
    base_url=https://your-instance.cloud.looker.com
    client_id=your_client_id
    client_secret=your_client_secret
"""

import argparse
import json
import re
import sys
import looker_sdk
from looker_sdk import models40 as models
from mappings import OLD_EXPLORE, NEW_MODEL, JOINED_VIEWS_IN_NEW_EXPLORE, FIELD_MAPS, FIELD_MAP


# ── populated at runtime ──────────────────────────────────────────────────────
_EXPLORE_VIEWS      = set()
_EXPLORE_VIEW_SETS  = {}
_EXPLORE_FIELD_SETS = {}


def parse_args():
    p = argparse.ArgumentParser(
        description="Copy specific tiles from a source dashboard to a destination dashboard"
    )
    p.add_argument("--source",     required=True, help="Source dashboard ID (read-only — never modified)")
    p.add_argument("--dest",       required=True, help="Destination dashboard ID (tiles are added here)")
    p.add_argument("--tiles",      required=True, nargs="+", metavar="TITLE",
                   help="Tile title(s) to copy from the source dashboard")
    p.add_argument("--ini",        default="looker.ini", help="Path to looker.ini (default: ./looker.ini)")
    p.add_argument("--production", action="store_true",
                   help="Run against production (skip dev session and git branch switch)")
    p.add_argument("--dry-run",    action="store_true",
                   help="Show what would be copied without writing anything")
    return p.parse_args()


# ── helpers ───────────────────────────────────────────────────────────────────

def extract_vis_config(element, query=None):
    vc = getattr(element, "vis_config", None)
    if vc and isinstance(vc, dict) and vc.get("type"):
        return vc
    rm = getattr(element, "result_maker", None)
    if rm:
        vc = getattr(rm, "vis_config", None)
        if vc and isinstance(vc, dict) and vc.get("type"):
            return vc
    if query:
        vc = getattr(query, "vis_config", None)
        if vc and isinstance(vc, dict) and vc.get("type"):
            return vc
    return None


def remap_fields(fields, field_map):
    if not fields:
        return fields
    return [field_map.get(f, f) for f in fields]


def remap_filters(filters, field_map):
    if not filters:
        return filters
    return {field_map.get(k, k): v for k, v in filters.items()}


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
    if not vc:
        return vc
    vc = dict(vc)
    for key in ("series_colors", "series_labels", "series_types", "series_point_styles",
                "series_collapsed", "series_cell_visualizations", "series_value_format",
                "series_text_format", "series_sizing", "series_axis_id", "series_error_type"):
        if isinstance(vc.get(key), dict):
            vc[key] = {field_map.get(k, k): v for k, v in vc[key].items()}
    for key in ("hidden_fields", "column_order", "hidden_pivots"):
        if isinstance(vc.get(key), list):
            vc[key] = [field_map.get(f, f) if isinstance(f, str) else f for f in vc[key]]
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


def is_problem_field(field, field_map, target_explore=None):
    if not field or "." not in field:
        return False
    if field in field_map:
        return False
    if target_explore and _EXPLORE_FIELD_SETS:
        return field not in _EXPLORE_FIELD_SETS.get(target_explore, set())
    if _EXPLORE_VIEWS:
        return field.split(".")[0] not in _EXPLORE_VIEWS
    return field.split(".")[0] not in JOINED_VIEWS_IN_NEW_EXPLORE


def broken_fields(fields, filters, dynamic_fields_str, field_map, target_explore=None):
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


def route_explore(source_explore, fields, explore_view_sets):
    candidates = [new_exp for (old, new_exp) in FIELD_MAPS if old == source_explore]
    if not candidates:
        raise ValueError(f"No FIELD_MAPS entry for source explore: {source_explore}")
    if len(candidates) == 1:
        return candidates[0]
    for f in (fields or []):
        if "." not in f:
            continue
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


def build_explore_view_sets(sdk):
    new_explores = {new_exp for (_, new_exp) in FIELD_MAPS}
    view_sets, field_sets = {}, {}
    for explore_name in new_explores:
        try:
            exp = sdk.lookml_model_explore(NEW_MODEL, explore_name, fields="fields")
            fields = (exp.fields.dimensions or []) + (exp.fields.measures or [])
            view_sets[explore_name]  = {f.name.split(".")[0] for f in fields}
            field_sets[explore_name] = {f.name for f in fields}
        except Exception:
            pass
    return view_sets, field_sets


# ── core logic ────────────────────────────────────────────────────────────────

def copy_tiles(sdk, source_id, dest_id, tile_names, dry_run=False):
    source_elements = sdk.dashboard_dashboard_elements(source_id)
    tile_names_set  = set(tile_names)

    target_elements = [el for el in source_elements if (el.title or "") in tile_names_set]
    missing = tile_names_set - {el.title or "" for el in target_elements}

    if missing:
        print(f"⚠️  Tiles not found on source dashboard {source_id}: {sorted(missing)}")

    if not target_elements:
        print("No matching tiles found on source. Exiting.")
        sys.exit(1)

    # Source layout positions for the target tiles
    src_layout = next(
        (l for l in sdk.dashboard_dashboard_layouts(str(source_id)) if getattr(l, "active", False)),
        None
    )
    src_pos = (
        {c.dashboard_element_id: c for c in (src_layout.dashboard_layout_components or [])}
        if src_layout else {}
    )

    dst_layout_id = next(
        (str(l.id) for l in sdk.dashboard_dashboard_layouts(str(dest_id)) if getattr(l, "active", False)),
        None
    )

    print(f"\nSource dashboard {source_id} → Destination dashboard {dest_id}")
    print(f"Copying {len(target_elements)} tile(s):")
    for el in target_elements:
        print(f"  • '{el.title or '(untitled)'}'")

    if dry_run:
        print("\n[DRY RUN — no writes]")
        for el in target_elements:
            if el.query_id:
                q = sdk.query(str(el.query_id))
                has_mapping = any(old == q.view for (old, _) in FIELD_MAPS)
                if has_mapping:
                    target_explore = route_explore(
                        q.view,
                        list(q.fields or []) + list((q.filters or {}).keys()),
                        _EXPLORE_VIEW_SETS,
                    )
                    field_map = FIELD_MAPS.get((q.view, target_explore), FIELD_MAP)
                    remapped  = [(f, field_map[f]) for f in (q.fields or []) if f in field_map]
                    print(f"\n  '{el.title}':")
                    if q.view != target_explore:
                        print(f"    explore: {q.view} → {target_explore}")
                    else:
                        print(f"    explore: {q.view} (unchanged)")
                    if remapped:
                        for old_f, new_f in remapped:
                            print(f"    field:   {old_f} → {new_f}")
                    else:
                        print(f"    fields:  (no remapping needed)")
                else:
                    print(f"\n  '{el.title}': no remapping (explore: {q.view})")
        return

    # ── copy each tile to dest ────────────────────────────────────────────────
    broken_summary = {}
    src_to_dest    = {}

    for el in target_elements:
        field_map = {}
        if el.query_id:
            q  = sdk.query(str(el.query_id))
            vc = extract_vis_config(el, q)
            has_mapping = any(old == q.view for (old, _) in FIELD_MAPS)
            if has_mapping:
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
                bad = broken_fields(new_query.fields, new_query.filters,
                                    new_query.dynamic_fields, field_map, new_query.view)
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

        print(f"  ✓ '{el.title or '(untitled)'}'")

    # ── apply source positions to new tiles ───────────────────────────────────
    if dst_layout_id and src_to_dest:
        dest_comps = {
            x.dashboard_element_id: x
            for x in (sdk.dashboard_layout(dst_layout_id).dashboard_layout_components or [])
        }
        for src_el_id, dest_el_id in src_to_dest.items():
            if dest_el_id in dest_comps:
                c = src_pos[src_el_id]
                sdk.update_dashboard_layout_component(
                    str(dest_comps[dest_el_id].id),
                    models.WriteDashboardLayoutComponent(
                        dashboard_layout_id=dst_layout_id,
                        dashboard_element_id=str(dest_el_id),
                        row=c.row,
                        column=c.column,
                        width=c.width,
                        height=c.height,
                    )
                )

    # ── summary ───────────────────────────────────────────────────────────────
    print(f"\n✓ Added {len(target_elements)} tile(s) to dashboard {dest_id}")
    if src_to_dest:
        print(f"  Positions copied from source layout. Reposition in Looker UI if needed.")
    if broken_summary:
        print(f"\n⚠️  {len(broken_summary)} tile(s) with unmapped fields:")
        for title, fields in broken_summary.items():
            print(f"  '{title}':")
            for f in fields:
                print(f"    {f}")


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()
    sdk  = looker_sdk.init40(config_file=args.ini)

    if not args.production:
        sdk.update_session(models.WriteApiSession(workspace_id="dev"))
        try:
            sdk.update_git_branch(project_id=NEW_MODEL,
                                  body=models.WriteGitBranch(name="v2-migration"))
        except Exception as e:
            print(f"⚠️  Could not switch to v2-migration branch (proceeding on current branch): {e}")

    try:
        _view_sets, _field_sets = build_explore_view_sets(sdk)
        _EXPLORE_VIEW_SETS.update(_view_sets)
        _EXPLORE_FIELD_SETS.update(_field_sets)
        _EXPLORE_VIEWS.update(set().union(*_view_sets.values()))
    except Exception as e:
        print(f"⚠️  Could not load explore fields: {e}")

    copy_tiles(sdk, args.source, args.dest, args.tiles, dry_run=args.dry_run)
    if not args.dry_run:
        print("\n✓ Done")
