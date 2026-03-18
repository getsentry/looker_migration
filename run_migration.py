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

# ─────────────────────────────────────────────
# EXPLORES — update for your migration
# ─────────────────────────────────────────────
OLD_EXPLORE = "product_facts"
NEW_MODEL   = "super_big_facts"
NEW_EXPLORE = "product_usage_org_proj"

# ─────────────────────────────────────────────
# VIEWS joined into NEW_EXPLORE
# Any field from a view NOT in this list will be flagged by --validate
# Add views here as you confirm they exist in product_usage_org_proj
# ─────────────────────────────────────────────
JOINED_VIEWS_IN_NEW_EXPLORE = {
    "product_facts_v2_base",
    "organizations",
    "organizations_age_tracking",
    "organizations_data_outcomes",
    "projects_data_outcomes",
    "engagement_score",
    "daily_financial_data_billing_category_struct",
    "subscriptions_v3",
}

# ─────────────────────────────────────────────
# FIELD MAP — fields that need remapping old → new
# Fields from joined views that haven't changed don't need to be listed here
# ─────────────────────────────────────────────
FIELD_MAP = {
    "product_facts.organizations_count":        "product_facts_v2_base.count",
    "product_facts.active_organizations_count": "organizations.active_organizations_count",
    "product_facts.dt_date":                    "product_facts_v2_base.dt_date",
    "product_facts.dt_month":                   "product_facts_v2_base.dt_month",
    "product_facts.is_last_day_of_month":       "product_facts_v2_base.is_last_day_of_month",
    "product_facts.is_last_day_of_week":        "product_facts_v2_base.is_last_day_of_week",
    "product_facts.org_age":                    "organizations_age_tracking.org_age",
    "product_facts.org_active":                 "organizations.is_active",
    "product_facts.organization_slug":          "organizations.slug",
    "product_facts.organization_id":            "product_facts_v2_base.organization_id",
    "product_facts.sum_org_active_users_28d":   "organizations.active_users_28d",
    "product_facts.sum_active_projects":        "organizations_data_outcomes.org_active_projects",
    "product_facts.sum_spans_accepted":         "projects_data_outcomes.daily_spans_accepted",
    "product_facts.sum_replays_accepted":       "projects_data_outcomes.replays_accepted",
    "product_facts.sum_errors_accepted":        "projects_data_outcomes.errors_accepted",
    # uptime monitor field renamed in new explore
    "organization_uptime_summary.org_dt_total_active_monitors": "organizations.org_dt_total_active_monitors",
    # logs field renamed in new explore
    "data_by_project.proj_logs_accepted":                       "projects_data_outcomes.logs_items_accepted",
    # metrics field mapped
    "data_by_project.proj_trace_metric_items_accepted":       "projects_data_outcomes.trace_metric_items_accepted",
    # TODO — find new field name for this before migrating dashboards that use it:
    # "project_uptime_details.total_active_monitors":           "???",
}


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Looker dashboard migration tool")
    p.add_argument("--source",        required=False, default=None, help="Source dashboard ID (copy FROM)")
    p.add_argument("--batch",         nargs="+", metavar="ID", help="Validate multiple source dashboard IDs (or SOURCE:DEST pairs)")
    p.add_argument("--dest",          required=False, default=None, help="Destination dashboard ID (copy TO)")
    p.add_argument("--dry-run",       action="store_true", help="Preview changes without writing")
    p.add_argument("--validate",      action="store_true", help="Check source dashboard tiles for unmapped fields")
    p.add_argument("--check-explore", action="store_true", help="Verify all FIELD_MAP destinations and JOINED_VIEWS exist in new explore")
    p.add_argument("--ini",           default="looker.ini", help="Path to looker.ini (default: ./looker.ini)")
    return p.parse_args()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def extract_vis_config(element):
    vc = getattr(element, "vis_config", None)
    if vc and isinstance(vc, dict) and vc.get("type"):
        return vc, "element.vis_config"
    rm = getattr(element, "result_maker", None)
    if rm:
        vc = getattr(rm, "vis_config", None)
        if vc and isinstance(vc, dict) and vc.get("type"):
            return vc, "result_maker.vis_config"
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
        if c.get("args"):
            c["args"] = [FIELD_MAP.get(a, a) if isinstance(a, str) else a for a in c["args"]]
    return json.dumps(customs)

def is_problem_field(field):
    """Returns True if a field needs to be flagged — it's from OLD_EXPLORE and unmapped,
    or from a view that isn't joined into the new explore."""
    if not field or "." not in field:
        return False
    view = field.split(".")[0]
    if field in FIELD_MAP:
        return False  # explicitly remapped, fine
    if view == OLD_EXPLORE:
        return True   # from old explore and not remapped
    if view not in JOINED_VIEWS_IN_NEW_EXPLORE:
        return True   # from a view not available in new explore
    return False





def check_explore(sdk, source_id):
    """Verify all FIELD_MAP destinations and JOINED_VIEWS exist in the new explore."""
    print(f"\n=== Checking fields exist in {NEW_MODEL}/{NEW_EXPLORE} ===")
    # Only check fields actually used in this dashboard
    elements = sdk.dashboard_dashboard_elements(source_id)
    used_old_fields = set()
    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        for f in (q.fields or []):
            used_old_fields.add(f)
        for f in (q.filters or {}).keys():
            used_old_fields.add(f)
        if q.dynamic_fields:
            try:
                for d in json.loads(q.dynamic_fields):
                    if d.get("based_on"):
                        used_old_fields.add(d["based_on"])
            except Exception:
                pass

    explore = sdk.lookml_model_explore(NEW_MODEL, NEW_EXPLORE, fields="fields")
    all_fields = set()
    for f in (explore.fields.dimensions or []):
        all_fields.add(f.name)
    for f in (explore.fields.measures or []):
        all_fields.add(f.name)
    all_views = {f.split(".")[0] for f in all_fields}
    # map of (old_field, new_field) -> list of tile names
    missing_fields = {}
    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        el_fields = set(q.fields or []) | set((q.filters or {}).keys())
        if q.dynamic_fields:
            try:
                for d in json.loads(q.dynamic_fields):
                    if d.get("based_on"):
                        el_fields.add(d["based_on"])
            except Exception:
                pass
        for old_field, new_field in FIELD_MAP.items():
            if old_field in el_fields and new_field not in all_fields:
                key = (old_field, new_field)
                tile_name = el.title or "(untitled tile)"
                missing_fields.setdefault(key, set()).add(tile_name)
    issues = []
    for (old_field, new_field), tiles in missing_fields.items():
        tile_list = ", ".join(f"'{t}'" for t in sorted(tiles))
        issues.append(f"  {old_field} -> ❌ {new_field} (used in: {tile_list})")
    for view in JOINED_VIEWS_IN_NEW_EXPLORE:
        if view not in all_views:
            issues.append(f"  JOINED_VIEWS_IN_NEW_EXPLORE view not found in explore: ❌ {view}")
    if issues:
        print("\u26a0\ufe0f  Issues found — check if these tiles matter to your migration:")
        for i in issues:
            print(i)
        return False
    print(f"  \u2705 All relevant mapped fields confirmed in new explore")
    return True

def validate(sdk, source_id):
    print(f"\n=== Validating source dashboard {source_id} ===")
    elements = sdk.dashboard_dashboard_elements(source_id)
    issues = []

    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        if q.view != OLD_EXPLORE:
            continue

        for f in (q.fields or []):
            if is_problem_field(f):
                issues.append(f"  Tile '{el.title}' — unmapped field: {f}")

        for f in (q.filters or {}).keys():
            if is_problem_field(f):
                issues.append(f"  Tile '{el.title}' — unmapped filter: {f}")

        for s in (q.sorts or []):
            field = s.split(" ")[0]
            if is_problem_field(field):
                issues.append(f"  Tile '{el.title}' — unmapped sort: {s}")

        if q.dynamic_fields:
            try:
                for d in json.loads(q.dynamic_fields):
                    label = d.get("label") or d.get("table_calculation") or "(unnamed)"
                    based_on = d.get("based_on", "")
                    if based_on and is_problem_field(based_on):
                        issues.append(f"  Tile '{el.title}' — dynamic field '{label}' based_on not available: {based_on}")
                    for ref in re.findall(r'\$\{([^}]+)\}', d.get("expression") or ""):
                        if is_problem_field(ref):
                            issues.append(f"  Tile '{el.title}' — dynamic field '{label}' expression references: {ref}")
                    for fk in (d.get("filters") or {}).keys():
                        if is_problem_field(fk):
                            issues.append(f"  Tile '{el.title}' — dynamic field '{label}' filter not available: {fk}")
            except Exception as e:
                issues.append(f"  Tile '{el.title}' — could not parse dynamic_fields: {e}")

    # Deduplicate
    seen, deduped = set(), []
    for i in issues:
        if i not in seen:
            seen.add(i)
            deduped.append(i)

    if deduped:
        print("⚠️  Issues found — resolve before migrating:")
        for i in deduped:
            print(i)
        print("\nTo fix: either add the field to FIELD_MAP, or add its view to JOINED_VIEWS_IN_NEW_EXPLORE if it exists in the new explore.")
        return False
    else:
        print("✅ All fields are mapped — safe to migrate")
        return True


# ─────────────────────────────────────────────
# STEP 1: Snapshot
# ─────────────────────────────────────────────
def snapshot(sdk, dest_id, dry_run):
    print(f"\n=== Step 1: Snapshot dashboard {dest_id} ===")
    elements = sdk.dashboard_dashboard_elements(dest_id)
    snapshot_data = []
    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        vc, loc = extract_vis_config(el)
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
# STEP 1b: Copy vis_config from source
# ─────────────────────────────────────────────
def copy_vis_config_from_source(sdk, source_id, dest_id, dry_run):
    print(f"\n=== Step 1b: Copy vis_config from source dashboard {source_id} ===")
    # Cache explore fields once for WILL BREAK checks
    try:
        _exp = sdk.lookml_model_explore(NEW_MODEL, NEW_EXPLORE, fields="fields")
        _explore_fields = set()
        for _f in (_exp.fields.dimensions or []):
            _explore_fields.add(_f.name)
        for _f in (_exp.fields.measures or []):
            _explore_fields.add(_f.name)
    except Exception:
        _explore_fields = set()

    source_elements = sdk.dashboard_dashboard_elements(source_id, fields="id,title,query_id,result_maker")
    dest_elements   = sdk.dashboard_dashboard_elements(dest_id)

    source_by_title    = {}
    source_by_query_id = {}
    for el in source_elements:
        vc, loc = extract_vis_config(el)
        if not vc:
            continue
        title_key = (el.title or "").strip().lower()
        if title_key:
            source_by_title[title_key] = (vc, loc, el)
        elif el.query_id:
            source_by_query_id[str(el.query_id)] = (vc, loc, el)

    print(f"  Found vis_config for {len(source_by_title) + len(source_by_query_id)} tiles in source")

    for el in dest_elements:
        if not el.query_id:
            continue
        title_key = (el.title or "").strip().lower()

        # Match by title first, then fall back to query_id for untitled tiles
        if title_key and title_key in source_by_title:
            vc, loc, src_el = source_by_title[title_key]
        elif not title_key and str(el.query_id) in source_by_query_id:
            vc, loc, src_el = source_by_query_id[str(el.query_id)]
        else:
            print(f"  ⚠️  '{el.title}' — no matching tile in source")
            continue
        src_total = src_row_total = None
        if src_el.query_id:
            src_q = sdk.query(str(src_el.query_id))
            src_total     = src_q.total
            src_row_total = src_q.row_total

        if dry_run:
            src_q = sdk.query(str(src_el.query_id)) if src_el.query_id else None
            if src_q:
                for f in (src_q.fields or []):
                    if is_problem_field(f):
                        print(f"  ❌ WILL BREAK '{el.title}' — field not in new explore: {f}")
                for f in (src_q.filters or {}).keys():
                    if is_problem_field(f):
                        print(f"  ❌ WILL BREAK '{el.title}' — filter not in new explore: {f}")
                if src_q.dynamic_fields:
                    try:
                        for d in json.loads(src_q.dynamic_fields):
                            label = d.get("label") or d.get("table_calculation") or "(unnamed)"
                            based_on = d.get("based_on", "")
                            if based_on:
                                if is_problem_field(based_on):
                                    print(f"  ❌ WILL BREAK '{el.title}' — dynamic field '{label}' based_on not in new explore: {based_on}")
                                elif based_on in FIELD_MAP and FIELD_MAP[based_on] not in _explore_fields:
                                    print(f"  ❌ WILL BREAK '{el.title}' — dynamic field '{label}' maps to missing field: {based_on} → {FIELD_MAP[based_on]}")
                    except Exception:
                        pass
            print(f"  [DRY RUN] Would copy {vc.get('type')} → '{el.title}' (total={src_total})")
            continue

        existing_query = sdk.query(str(el.query_id))
        new_query = sdk.create_query(
            models.WriteQuery(
                model=existing_query.model,
                view=existing_query.view,
                fields=existing_query.fields,
                filters=existing_query.filters,
                sorts=existing_query.sorts,
                limit=existing_query.limit,
                dynamic_fields=existing_query.dynamic_fields,
                pivots=existing_query.pivots,
                vis_config=vc,
                total=src_total,
                row_total=src_row_total,
                filter_config=None,  # must be null per API docs to avoid unexpected filtering
            )
        )
        sdk.update_dashboard_element(
            str(el.id),
            models.WriteDashboardElement(query_id=new_query.id)
        )
        print(f"  ✅ '{el.title}' — copied {vc.get('type')} (total={src_total})")


# ─────────────────────────────────────────────
# STEP 2: Fix dashboard filters
# ─────────────────────────────────────────────
def fix_dashboard_filters(sdk, dest_id, dry_run):
    print("\n=== Step 2: Fix dashboard filters ===")
    dashboard = sdk.dashboard(dest_id)
    for f in (dashboard.dashboard_filters or []):
        if f.dimension in FIELD_MAP:
            new_field = FIELD_MAP[f.dimension]
            if dry_run:
                print(f"  [DRY RUN] Would update '{f.title}': {f.dimension} → {new_field}, explore → {NEW_EXPLORE}")
            else:
                sdk.update_dashboard_filter(
                    str(f.id),
                    models.WriteDashboardFilter(dimension=new_field, explore=NEW_EXPLORE)
                )
                print(f"  ✓ Updated '{f.title}': {f.dimension} → {new_field}, explore → {NEW_EXPLORE}")
        else:
            print(f"  OK '{f.title}': {f.dimension} (explore: {f.explore})")


# ─────────────────────────────────────────────
# STEP 3: Swap explore + remap fields
# ─────────────────────────────────────────────
def swap_and_fix_tiles(sdk, dest_id, dry_run):
    print("\n=== Step 3: Swap explore + remap fields ===")
    elements = sdk.dashboard_dashboard_elements(dest_id)
    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        if q.view != OLD_EXPLORE:
            print(f"  Skipping '{el.title}' — already on: {q.view}")
            continue
        vc, _ = extract_vis_config(el)
        if dry_run:
            # Check for fields that would break even in dry run mode
            for f in (q.fields or []):
                if is_problem_field(f):
                    print(f"  ⚠️  WILL BREAK '{el.title}' — field not available in new explore: {f}")
            for f in (q.filters or {}).keys():
                if is_problem_field(f):
                    print(f"  ⚠️  WILL BREAK '{el.title}' — filter not available in new explore: {f}")
            for s in (q.sorts or []):
                if is_problem_field(s.split(" ")[0]):
                    print(f"  ⚠️  WILL BREAK '{el.title}' — sort not available in new explore: {s}")
            print(f"  [DRY RUN] Would swap '{el.title}'")
            continue
        new_query = sdk.create_query(
            models.WriteQuery(
                model=NEW_MODEL,
                view=NEW_EXPLORE,
                fields=remap_fields(q.fields, el.title),
                filters=remap_filters(q.filters, el.title),
                sorts=remap_sorts(q.sorts),
                limit=q.limit,
                dynamic_fields=remap_dynamic_fields(q.dynamic_fields),
                pivots=remap_fields(q.pivots),
                vis_config=vc,
                total=q.total,
                row_total=q.row_total,
                filter_config=None,  # must be null per API docs
            )
        )
        sdk.update_dashboard_element(
            str(el.id),
            models.WriteDashboardElement(query_id=new_query.id)
        )
        print(f"  ✅ Swapped '{el.title}'")


# ─────────────────────────────────────────────
# STEP 4: Reconnect dashboard filters to tiles
# Copies filter listen mappings from source, remapping old fields to new
# ─────────────────────────────────────────────
def reconnect_dashboard_filters(sdk, source_id, dest_id, dry_run):
    print("\n=== Step 4: Reconnect dashboard filters to tiles ===")

    source_elements = sdk.dashboard_dashboard_elements(source_id)
    dest_elements   = sdk.dashboard_dashboard_elements(dest_id)

    # Build lookup of source filterables by title
    source_filterables = {}
    for el in source_elements:
        if not el.result_maker:
            continue
        title_key = (el.title or "").strip().lower()
        if title_key:
            source_filterables[title_key] = el.result_maker.filterables or []

    for el in dest_elements:
        if not el.query_id or not el.result_maker:
            continue

        title_key = (el.title or "").strip().lower()
        if title_key not in source_filterables:
            continue

        src_filterables = source_filterables[title_key]
        if not src_filterables:
            continue

        # Remap old field names to new ones in the listen mappings
        needs_update = False
        new_filterables = []
        for filterable in src_filterables:
            new_listens = []
            for listen in (filterable.listen or []):
                old_field = listen.field
                new_field = FIELD_MAP.get(old_field, old_field)
                if new_field != old_field:
                    needs_update = True
                    print(f"  ⚠️  '{el.title}': remapping filter '{listen.dashboard_filter_name}' {old_field} → {new_field}")
                new_listens.append(
                    models.ResultMakerFilterablesListen(
                        dashboard_filter_name=listen.dashboard_filter_name,
                        field=new_field
                    )
                )
            new_filterables.append(
                models.ResultMakerFilterables(
                    model=filterable.model,
                    view=filterable.view,
                    name=filterable.name,
                    listen=new_listens
                )
            )

        if not needs_update:
            print(f"  OK '{el.title}' — filter mappings already correct")
            continue

        if dry_run:
            print(f"  [DRY RUN] Would remap filter fields for '{el.title}'")
            continue

        sdk.update_dashboard_element(
            str(el.id),
            models.WriteDashboardElement(
                result_maker=models.WriteResultMakerWithIdVisConfigAndDynamicFields(
                    filterables=new_filterables
                )
            )
        )
        print(f"  ✅ '{el.title}' — filter fields remapped")


# ─────────────────────────────────────────────
# STEP 5: Verify
# ─────────────────────────────────────────────
def verify(sdk, dest_id):
    print("\n=== Step 5: Verify ===")
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
        vc, loc = extract_vis_config(el)
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
    sdk.update_session(models.WriteApiSession(workspace_id="dev"))
    sdk.update_git_branch(project_id="super_big_facts", body=models.WriteGitBranch(name="v2-migration"))

    dry_run   = args.dry_run
    # --batch: validate multiple dashboards, deduped missing fields
    if args.batch:
        from collections import defaultdict
        missing = defaultdict(lambda: {"new_field": None, "dashboards": defaultdict(set)})
        statuses = {}

        print(f"\n=== Batch Pre-Migration Check: {len(args.batch)} dashboards ===\n")

        # Load explore fields once
        explore = sdk.lookml_model_explore(NEW_MODEL, NEW_EXPLORE, fields="fields")
        all_explore_fields = set()
        for f in (explore.fields.dimensions or []):
            all_explore_fields.add(f.name)
        for f in (explore.fields.measures or []):
            all_explore_fields.add(f.name)

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
                el_fields = set(q.fields or []) | set((q.filters or {}).keys())
                if q.dynamic_fields:
                    try:
                        for d in json.loads(q.dynamic_fields):
                            if d.get("based_on"):
                                el_fields.add(d["based_on"])
                    except Exception:
                        pass
                # Skip tiles not on the old product_facts explore — they are unrelated
                if q.view != OLD_EXPLORE:
                    continue
                tile = el.title or "(untitled)"
                # Collect based_on fields from dynamic fields (table calcs, custom measures)
                based_on_fields = set()
                if q.dynamic_fields:
                    try:
                        for d in json.loads(q.dynamic_fields):
                            if d.get("based_on"):
                                based_on_fields.add(d["based_on"])
                    except Exception:
                        pass
                # Only check real LookML fields (view.field format), skip calc names
                lookml_fields = {f for f in el_fields if "." in f}
                lookml_fields |= based_on_fields
                for f in lookml_fields:
                    if f not in FIELD_MAP and f.split(".")[0] not in JOINED_VIEWS_IN_NEW_EXPLORE:
                        missing[f]["new_field"] = None
                        missing[f]["dashboards"][label].add(tile)
                        dashboard_issues = True
                for old_field, new_field in FIELD_MAP.items():
                    if old_field in lookml_fields and new_field not in all_explore_fields:
                        missing[old_field]["new_field"] = new_field
                        missing[old_field]["dashboards"][label].add(tile)
                        dashboard_issues = True

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

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Migrating dashboard {source_id} → {dest_id}")

    # --check-explore: verify FIELD_MAP destinations and JOINED_VIEWS exist in new explore
    if args.check_explore:
        ok = check_explore(sdk, source_id)
        sys.exit(0 if ok else 1)

    # --validate: check source dashboard tiles for unmapped fields
    if args.validate:
        ok = validate(sdk, source_id)
        sys.exit(0 if ok else 1)

    # full migration (dry-run or live)
    snapshot(sdk, dest_id, dry_run)
    copy_vis_config_from_source(sdk, source_id, dest_id, dry_run)
    fix_dashboard_filters(sdk, dest_id, dry_run)
    swap_and_fix_tiles(sdk, dest_id, dry_run)
    reconnect_dashboard_filters(sdk, source_id, dest_id, dry_run)
    verify(sdk, dest_id)
    print(f"\n✓ Done — snapshot saved to snapshot_{dest_id}.json")
