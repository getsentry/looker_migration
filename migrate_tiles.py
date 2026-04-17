"""
Looker Tile Migration: product_usage_org_proj → product_usage_sdk
==================================================================
Updates specific tiles on a dashboard in-place, switching their explore
from product_usage_org_proj to product_usage_sdk and remapping fields.

USAGE:
  python3 migrate_tiles.py --dashboard 1234 --tiles "Tile A" "Tile B"
  python3 migrate_tiles.py --dashboard 1234 --tiles "Tile A" --dry-run
  python3 migrate_tiles.py --dashboard 1234 --tiles "Tile A" --ini ~/my_looker.ini --production

CREDENTIALS:
  Set in looker.ini:
    [Looker]
    base_url=https://your-instance.cloud.looker.com
    client_id=your_client_id
    client_secret=your_client_secret
"""

import argparse
import json
import sys
import looker_sdk
from looker_sdk import models40 as models
from mappings import NEW_MODEL, FIELD_MAPS


SOURCE_EXPLORE = "product_usage_org_proj"
TARGET_EXPLORE = "product_usage_sdk"


def parse_args():
    p = argparse.ArgumentParser(
        description="Migrate specific dashboard tiles from product_usage_org_proj to product_usage_sdk"
    )
    p.add_argument("--dashboard",  required=True, help="Dashboard ID to update")
    p.add_argument("--tiles",      required=True, nargs="+", metavar="TITLE",
                   help="One or more tile titles to migrate")
    p.add_argument("--ini",        default="looker.ini",
                   help="Path to looker.ini (default: ./looker.ini)")
    p.add_argument("--production", action="store_true",
                   help="Run against production (skip dev session and git branch switch)")
    p.add_argument("--dry-run",    action="store_true",
                   help="Show what would change without writing anything")
    return p.parse_args()


def build_field_map():
    """Derive a field map: product_usage_org_proj fields → product_usage_sdk fields.

    Bridges through the shared product_facts source:
      product_facts.X → org_proj_field  (from org_proj map)
      product_facts.X → sdk_field       (from sdk map)
    ⟹ org_proj_field → sdk_field

    Only entries where the field name actually changes are included.
    Fields with the same name in both explores pass through unchanged.
    """
    org_proj_map = FIELD_MAPS.get(("product_facts", "product_usage_org_proj"), {})
    sdk_map      = FIELD_MAPS.get(("product_facts", "product_usage_sdk"), {})
    inv_org_proj = {v: k for k, v in org_proj_map.items()}  # org_proj_field → pf_field
    return {
        org_proj_field: sdk_map[pf_field]
        for org_proj_field, pf_field in inv_org_proj.items()
        if pf_field in sdk_map and org_proj_field != sdk_map[pf_field]
    }


# ── field / vis remapping helpers ─────────────────────────────────────────────

def remap_list(items, field_map):
    if not items:
        return items
    return [field_map.get(f, f) for f in items]


def remap_dict_keys(d, field_map):
    if not d:
        return d
    return {field_map.get(k, k): v for k, v in d.items()}


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


def remap_dynamic_fields(dynamic_fields_str, field_map):
    if not dynamic_fields_str:
        return dynamic_fields_str
    customs = json.loads(dynamic_fields_str)
    for c in customs:
        if c.get("based_on") in field_map:
            c["based_on"] = field_map[c["based_on"]]
        if c.get("filters"):
            c["filters"] = remap_dict_keys(c["filters"], field_map)
        for expr_key in ("expression", "filter_expression"):
            if c.get(expr_key):
                for old, new in field_map.items():
                    c[expr_key] = c[expr_key].replace("${" + old + "}", "${" + new + "}")
        if c.get("args"):
            c["args"] = [field_map.get(a, a) if isinstance(a, str) else a for a in c["args"]]
    return json.dumps(customs)


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
            vc[key] = [field_map.get(f, f) for f in vc[key]]
    return vc


def extract_vis_config(element, query=None):
    for source in (element, getattr(element, "result_maker", None), query):
        if source:
            vc = getattr(source, "vis_config", None)
            if vc and isinstance(vc, dict) and vc.get("type"):
                return vc
    return None


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()
    sdk = looker_sdk.init40(config_file=args.ini)

    if not args.production:
        sdk.update_session(models.WriteApiSession(workspace_id="dev"))
        try:
            sdk.update_git_branch(project_id=NEW_MODEL, body=models.WriteGitBranch(name="v2-migration"))
        except Exception as e:
            print(f"⚠️  Could not switch to v2-migration branch (proceeding on current branch): {e}")

    field_map    = build_field_map()
    tile_names   = set(args.tiles)
    dashboard_id = args.dashboard

    elements        = sdk.dashboard_dashboard_elements(dashboard_id)
    target_elements = [el for el in elements if (el.title or "") in tile_names]

    missing = tile_names - {el.title or "" for el in target_elements}
    if missing:
        print(f"⚠️  Tiles not found on dashboard {dashboard_id}: {sorted(missing)}")

    if not target_elements:
        print("No matching tiles found. Exiting.")
        sys.exit(1)

    print(f"\nDashboard {dashboard_id}: targeting {len(target_elements)} tile(s) "
          f"({SOURCE_EXPLORE} → {TARGET_EXPLORE})")
    if args.dry_run:
        print("  [DRY RUN — no writes]\n")

    migrated, skipped = [], []

    for el in target_elements:
        title = el.title or f"(id={el.id})"

        if not el.query_id:
            skipped.append((title, "no query_id"))
            continue

        q = sdk.query(str(el.query_id))

        if q.view != SOURCE_EXPLORE:
            skipped.append((title, f"explore is '{q.view}', not '{SOURCE_EXPLORE}'"))
            continue

        vc          = extract_vis_config(el, q)
        new_fields  = remap_list(q.fields, field_map)
        new_filters = remap_dict_keys(q.filters, field_map)
        new_sorts   = remap_sorts(q.sorts, field_map)
        new_dyn     = remap_dynamic_fields(q.dynamic_fields, field_map)
        new_pivots  = remap_list(q.pivots, field_map)
        new_vc      = remap_vis_config(vc, field_map)

        if args.dry_run:
            changed = [(o, n) for o, n in zip(q.fields or [], new_fields or []) if o != n]
            print(f"  '{title}':")
            print(f"    explore: {SOURCE_EXPLORE} → {TARGET_EXPLORE}")
            if changed:
                for old_f, new_f in changed:
                    print(f"    field:   {old_f} → {new_f}")
            else:
                print(f"    fields:  (no remapping needed for listed fields)")
            print()
            migrated.append(title)
            continue

        new_query = sdk.create_query(models.WriteQuery(
            model=NEW_MODEL,
            view=TARGET_EXPLORE,
            fields=new_fields,
            filters=new_filters,
            sorts=new_sorts,
            limit=q.limit,
            dynamic_fields=new_dyn,
            pivots=new_pivots,
            vis_config=new_vc,
            total=q.total,
            row_total=q.row_total,
            filter_config=None,
        ))

        sdk.update_dashboard_element(str(el.id), models.WriteDashboardElement(
            query_id=new_query.id,
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
            sdk.update_dashboard_element(str(el.id), models.WriteDashboardElement(
                result_maker=models.WriteResultMakerWithIdVisConfigAndDynamicFields(
                    filterables=remapped_filterables)
            ))

        migrated.append(title)
        print(f"  ✓ '{title}'")

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Done: "
          f"{len(migrated)} migrated, {len(skipped)} skipped")
    if skipped:
        for title, reason in skipped:
            print(f"  skipped '{title}': {reason}")
