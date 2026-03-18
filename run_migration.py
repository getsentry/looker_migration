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
    # === 390 new mappings ===
    "seer_analytic_event_usage.issue_details_viewed": "product_facts_v2.users_seer.issue_details_viewed",
    "product_facts.dt_week": "product_facts_v2.organizations_age_tracking.dt_week",
    "per_product_trial_flags.in_seer_trial": "product_facts_v2.organizations_seer.in_seer_trial",
    "sentry_organizationoptions_new.seer_code_review_beta": "product_facts_v2.organizations_feature_flags.seer_code_review_beta",
    "new_seer_orgs.estimated_seats": "product_facts_v2.organizations_seer.estimated_seats",
    "product_facts_org_seer_user_rollup.billable_seats_28d": "product_facts_v2.users_seer.billable_seats_28d",
    "seer_analytic_event_usage.solution_code": "product_facts_v2.users_seer.solution_code",
    "seer_analytic_event_usage.root_cause_find_solution": "product_facts_v2.users_seer.root_cause_find_solution",
    "seer_analytic_event_usage.issue_viewed_with_solution": "product_facts_v2.users_seer.issue_viewed_with_solution",
    "seer_analytic_event_usage.create_pr_clicked": "product_facts_v2.users_seer.create_pr_clicked",
    "seer_analytic_event_usage.push_to_branch_or_pr_setup": "product_facts_v2.users_seer.push_to_branch_or_pr_setup",
    "seer_analytic_event_usage.deep_dive_panel_views": "product_facts_v2.users_seer.deep_dive_panel_views",
    "seer_analytic_event_usage.agent_instructions_given": "product_facts_v2.users_seer.agent_instructions_given",
    "seer_analytic_event_usage.start_fix_clicked": "product_facts_v2.users_seer.start_fix_clicked",
    "seer_analytic_event_usage.issue_viewed_with_rca": "product_facts_v2.users_seer.issue_viewed_with_rca",
    "org_feedback_received.total_comments": "product_facts_v2.organizations_feedback_received.total_comments",
    "org_feedback_received.total_fixes_applied": "product_facts_v2.organizations_feedback_received.total_fixes_applied",
    "org_feedback_received.total_fixes_rejected": "product_facts_v2.organizations_feedback_received.total_fixes_rejected",
    "org_feedback_received.total_downvotes": "product_facts_v2.organizations_feedback_received.total_downvotes",
    "org_feedback_received.total_upvotes": "product_facts_v2.organizations_feedback_received.total_upvotes",
    "org_feedback_received.total_hearts": "product_facts_v2.organizations_feedback_received.total_hearts",
    "user_details.given_name": "product_facts_v2.users_seer.given_name",
    "seer_analytic_event_usage.pr_merged": "product_facts_v2.users_seer.pr_merged",
    "seer_analytic_event_usage.total_usage": "product_facts_v2.users_seer.total_usage",
    "seer_analytic_event_usage.user_id": "product_facts_v2.users_seer.user_id",
    "user_details.full_name": "product_facts_v2.users_seer.full_name",
    "subscriptions_v3.total_churn_arr": "financial_data.daily_arr_combined.total_churn_arr",
    "product_facts.orgs_issue_details_viewed_has_root_cause_true": "product_facts_v2.organizations_seer.orgs_issue_details_viewed_has_root_cause_true",
    "product_facts.orgs_found_solution": "product_facts_v2.organizations_seer.orgs_found_solution",
    "product_facts.orgs_autofix_pr_merged": "product_facts_v2.organizations_seer.orgs_autofix_pr_merged",
    "new_seer_orgs.is_new_seer_org": "product_facts_v2.organizations_seer.is_new_seer_org",
    "new_seer_orgs.seer_enabled_week": "product_facts_v2.organizations_seer.seer_enabled_week",
    "new_seer_orgs.total_invoiced_seats": "product_facts_v2.organizations_seer.total_invoiced_seats",
    "new_seer_orgs.seer_churn_date": "product_facts_v2.organizations_seer.seer_churn_date",
    "new_seer_orgs.seer_enabled_date": "product_facts_v2.organizations_seer.seer_enabled_date",
    "new_seer_orgs.seats_at_churn": "product_facts_v2.organizations_seer.seats_at_churn",
    "product_facts_org_seer.total_events_sum": "product_facts_v2.organizations_seer.total_events_sum",
    "seer_analytic_event_usage.is_most_active_seer_user": "product_facts_v2.users_seer.is_most_active_seer_user",
    "product_facts_org_seer.total_unique_prs_sum": "product_facts_v2.organizations_seer.total_unique_prs_sum",
    "product_facts_org_seer.total_unique_repos_sum": "product_facts_v2.organizations_seer.total_unique_repos_sum",
    "new_seer_orgs.seer_enabled_month": "product_facts_v2.organizations_seer.seer_enabled_month",
    "org_feedback_received.prs_with_feedback": "product_facts_v2.organizations_feedback_received.prs_with_feedback",
    "user_details.username": "product_facts_v2.users_seer.username",
    "product_facts_org_seer.total_unique_repos": "product_facts_v2.organizations_seer.total_unique_repos",
    "product_facts.seer_cohort": "product_facts_v2.organizations_seer.seer_cohort",
    "product_facts.seer_filter_mode": "product_facts_v2.organizations_seer.seer_filter_mode",
    "product_facts.seer_segment_match": "product_facts_v2.organizations_seer.seer_segment_match",
    "product_facts_org_seer_user_rollup.is_billable_any_seat_28d": "product_facts_v2.users_seer.is_billable_any_seat_28d",
    "product_facts_org_rca_feedback.rca_negative_feedback": "product_facts_v2.organizations_feedback_received.rca_negative_feedback",
    "product_facts_org_rca_feedback.rca_positive_feedback": "product_facts_v2.organizations_feedback_received.rca_positive_feedback",
    "product_facts_org_rca_feedback.changes_negative_feedback": "product_facts_v2.organizations_feedback_received.changes_negative_feedback",
    "product_facts_org_rca_feedback.changes_positive_feedback": "product_facts_v2.organizations_feedback_received.changes_positive_feedback",
    "product_facts_org_rca_feedback.solution_positive_feedback": "product_facts_v2.organizations_feedback_received.solution_positive_feedback",
    "product_facts_org_rca_feedback.solution_negative_feedback": "product_facts_v2.organizations_feedback_received.solution_negative_feedback",
    "product_facts.seer_billable_seats_28d": "product_facts_v2.organizations_seer.seer_billable_seats_28d",
    "seer_usage_cost.total_dollar_cost": "product_facts_v2.organizations_seer.total_dollar_cost",
    "seer_usage_cost.step": "product_facts_v2.organizations_seer.step",
    "seer_usage_cost.feature": "product_facts_v2.organizations_seer.feature",
    "seer_pr_events.distinct_prs": "product_facts_v2.organizations_seer.distinct_prs",
    "product_facts.trace_metric_items_accepted_28d": "product_facts_v2.organizations_events.trace_metric_items_accepted_28d",
    "metric_type_events.metric_items": "product_facts_v2.projects_data_outcomes.metric_items",
    "metric_type_events.metric_bytes": "product_facts_v2.projects_data_outcomes.metric_bytes",
    "data_by_sdk.trace_metric_size_bytes_28d": "product_facts_v2.sdk_base_events.trace_metric_size_bytes_28d",
    "data_by_sdkversion.trace_metric_items": "product_facts_v2.sdk_base_events.trace_metric_items",
    "data_by_sdk.trace_metric_items_28d": "product_facts_v2.sdk_base_events.trace_metric_items_28d",
    "data_by_sdk.sdk_name": "product_facts_v2.sdk_base_events.sdk_name",
    "data_by_sdkversion.trace_metric_size_bytes": "product_facts_v2.sdk_base_events.trace_metric_size_bytes",
    "data_by_sdkversion.median_metric_size_bytes": "product_facts_v2.sdk_base_events.median_metric_size_bytes",
    "metric_type_events.median_size_bytes": "product_facts_v2.projects_data_outcomes.median_size_bytes",
    "metric_type_events.median_attributes": "product_facts_v2.projects_data_outcomes.median_attributes",
    "data_by_project.proj_trace_metric_items_accepted": "product_facts_v2.projects_base_table.proj_trace_metric_items_accepted",
    "product_facts.org_ea_flag": "product_facts_v2.organizations_analytics_summary.org_ea_flag",
    "product_facts.is_last_day_of_fiscal_quarter": "product_facts_v2.organizations_age_tracking.is_last_day_of_fiscal_quarter",
    "product_facts.dt_fiscal_quarter": "product_facts_v2.organizations_age_tracking.dt_fiscal_quarter",
    "install_base_cohorts.platform_only_errors_engaged": "product_facts_v2.organizations_analytics_summary.platform_only_errors_engaged",
    "install_base_cohorts._27k_23k": "product_facts_v2.organizations_analytics_summary._27k_23k",
    "install_base_cohorts.top_paid_ss": "product_facts_v2.organizations_analytics_summary.top_paid_ss",
    "install_base_cohorts.top_ai": "product_facts_v2.organizations_analytics_summary.top_ai",
    "install_base_cohorts.js_cohort": "product_facts_v2.organizations_analytics_summary.js_cohort",
    "install_base_cohorts.other_ai": "product_facts_v2.organizations_analytics_summary.other_ai",
    "install_base_cohorts.higher_tier": "product_facts_v2.organizations_analytics_summary.higher_tier",
    "install_base_cohorts.top_tracing": "product_facts_v2.organizations_analytics_summary.top_tracing",
    "install_base_cohorts.likely_ai_adoption": "product_facts_v2.organizations_analytics_summary.likely_ai_adoption",
    "install_base_cohorts.power_users": "product_facts_v2.organizations_analytics_summary.power_users",
    "install_base_cohorts.sales_led_cx": "product_facts_v2.organizations_analytics_summary.sales_led_cx",
    "install_base_cohorts.fortune_global_mobile_apps": "product_facts_v2.organizations_analytics_summary.fortune_global_mobile_apps",
    "product_facts_promocodeclaimant.date_added_month": "product_facts_v2.organizations_promocode_usage.date_added_month",
    "product_facts_promocodeclaimant.date_added_date": "product_facts_v2.organizations_promocode_usage.date_added_date",
    "product_facts_promocodeclaimant.promocode": "product_facts_v2.organizations_promocode_usage.promocode",
    "product_facts_promocodeclaimant.date_added_week": "product_facts_v2.organizations_promocode_usage.date_added_week",
    "product_facts_promocodeclaimant.promocode_id": "product_facts_v2.organizations_promocode_usage.promocode_id",
    "subscriptions_v3.total_new_arr": "financial_data.daily_arr_combined.total_new_arr",
    "subscriptions_v3.total_expansion_arr": "financial_data.daily_arr_combined.total_expansion_arr",
    "product_facts.replays_accepted_28d": "product_facts_v2.organizations_data_outcomes.replays_accepted_28d",
    "product_facts.errors_accepted_28d": "product_facts_v2.organizations_data_outcomes.errors_accepted_28d",
    "product_facts.transactions_accepted_28d": "product_facts_v2.organizations_data_outcomes.transactions_accepted_28d",
    "product_facts.replays_utilization_rate": "product_facts_v2.organizations_data_outcomes.replays_utilization_rate",
    "product_facts.spans_utilization_rate": "product_facts_v2.organizations_events.spans_utilization_rate",
    "product_facts.transactions_utilization_rate": "product_facts_v2.organizations_data_outcomes.transactions_utilization_rate",
    "product_facts.errors_utilization_rate": "product_facts_v2.organizations_data_outcomes.errors_utilization_rate",
    "product_facts.logs_accepted_28d": "product_facts_v2.organizations_events.logs_accepted_28d",
    "product_facts.org_active_users_28d": "product_facts_v2.organizations_age_tracking.org_active_users_28d",
    "product_facts.spans_accepted_28d": "product_facts_v2.organizations_events.spans_accepted_28d",
    "billing_model.sum_total_churn_arr": "financial_data.daily_arr_by_category.sum_total_churn_arr",
    "sentry_organizationoptions_new.size_analysis_beta_org": "product_facts_v2.organizations_feature_flags.size_analysis_beta_org",
    "product_facts_emerge.size_builds_28d": "product_facts_v2.organizations_emerge.size_builds_28d",
    "product_facts_emerge.size_builds_total": "product_facts_v2.organizations_emerge.size_builds_total",
    "emerge_project.size_builds_total": "product_facts_v2.projects_emerge.size_builds_total",
    "emerge_project.size_builds_28d": "product_facts_v2.projects_emerge.size_builds_28d",
    "data_by_project.project_platform": "product_facts_v2.projects_base_table.project_platform",
    "product_facts.sum_org_users_total": "product_facts_v2.organizations_analytics_summary.sum_org_users_total",
    "product_facts_emerge.distribution_builds_28d": "product_facts_v2.organizations_emerge.distribution_builds_28d",
    "product_facts_emerge.distribution_builds_total": "product_facts_v2.organizations_emerge.distribution_builds_total",
    "product_facts_emerge.distribution_installs_total": "product_facts_v2.organizations_emerge.distribution_installs_total",
    "product_facts_emerge.distribution_installs_28d": "product_facts_v2.organizations_emerge.distribution_installs_28d",
    "emerge_first_adoption_dates.first_size_analysis_date_week": "product_facts_v2.organizations_emerge.first_size_analysis_date_week",
    "emerge_first_adoption_dates.first_size_analysis_date_date": "product_facts_v2.organizations_emerge.first_size_analysis_date_date",
    "emerge_first_adoption_dates.first_distribution_build_date_week": "product_facts_v2.organizations_emerge.first_distribution_build_date_week",
    "emerge_first_adoption_dates.first_distribution_build_date_date": "product_facts_v2.organizations_emerge.first_distribution_build_date_date",
    "product_facts.crons_active_monitor_1d": "product_facts_v2.organizations_cron_monitoring.crons_active_monitor_1d",
    "product_facts_events.sdk_family": "product_facts_v2.organizations_events.sdk_family",
    "product_facts.performance_units_accepted": "product_facts_v2.organizations_data_outcomes.performance_units_accepted",
    "product_facts_events.event_type": "product_facts_v2.organizations_events.event_type",
    "accounts_billingmetricusage_on_org.outcome_readable": "financial_data.accounts_billingmetricusage_on_org.outcome_readable",
    "organization_uptime_summary.org_dt_total_active_monitors": "product_facts_v2.organizations_uptime_monitoring.org_dt_total_active_monitors",
    "data_by_sdkversion.total_profile_duration_continuous": "product_facts_v2.sdk_base_events.total_profile_duration_continuous",
    "data_by_sdkversion.profile_duration_frontend": "product_facts_v2.sdk_base_events.profile_duration_frontend",
    "data_by_sdkversion.profile_duration_backend": "product_facts_v2.sdk_base_events.profile_duration_backend",
    "product_facts.combined_integration_features": "product_facts_v2.organizations_feature_flags.combined_integration_features",
    "data_by_project.project_name": "product_facts_v2.projects_base_table.project_name",
    "data_by_project.project_id": "product_facts_v2.projects_base_table.project_id",
    "data_by_project.project_count": "product_facts_v2.projects_base_table.project_count",
    "data_by_project.proj_errors_accepted": "product_facts_v2.projects_base_table.proj_errors_accepted",
    "data_by_project.has_environments_current_enabled": "product_facts_v2.projects_base_table.has_environments_current_enabled",
    "data_by_project.primary_sdk": "product_facts_v2.projects_base_table.primary_sdk",
    "product_facts.org_active_backend": "product_facts_v2.organizations_feature_flags.org_active_backend",
    "sentry_project.slug": "product_facts_v2.projects_base_table.slug",
    "product_facts_events.number_of_events_sum": "product_facts_v2.organizations_events.number_of_events_sum",
    "billing_model_billing_category.sum_contraction_arr": "financial_data.daily_arr_by_category.sum_contraction_arr",
    "billing_model_billing_category.sum_reactivation_arr": "financial_data.daily_arr_by_category.sum_reactivation_arr",
    "daily_financial_data_billing_category_struct.change_in_ondemand_arr": "financial_data.daily_financial_data_billing_category.change_in_ondemand_arr",
    "billing_model_billing_category.sum_churn_arr": "financial_data.daily_arr_by_category.sum_churn_arr",
    "billing_model_billing_category.sum_new_arr": "financial_data.daily_arr_by_category.sum_new_arr",
    "billing_model.billing_model": "financial_data.daily_arr_by_category.billing_model",
    "billing_model_billing_category.sum_expansion_arr": "financial_data.daily_arr_by_category.sum_expansion_arr",
    "product_facts.first_continuous_profile_date_date": "product_facts_v2.organizations_feature_adoption_dates.first_continuous_profile_date_date",
    "product_facts.profile_duration_accepted_28d": "product_facts_v2.organizations_events.profile_duration_accepted_28d",
    "product_facts.frontend_profile_duration_accepted_28d": "product_facts_v2.organizations_events.frontend_profile_duration_accepted_28d",
    "product_facts.first_ui_profile_date_date": "product_facts_v2.organizations_feature_adoption_dates.first_ui_profile_date_date",
    "product_facts.front_end_profile_duration_accepted_sum": "product_facts_v2.organizations_events.front_end_profile_duration_accepted_sum",
    "product_facts.profile_duration_accepted_sum": "product_facts_v2.organizations_events.profile_duration_accepted_sum",
    "product_facts.total_profile_duration_accepted_sum": "product_facts_v2.organizations_events.total_profile_duration_accepted_sum",
    "data_by_sdk.pduration_frontend_28d_sdk_family": "product_facts_v2.sdk_base_events.pduration_frontend_28d_sdk_family",
    "data_by_sdk.sdk_family": "product_facts_v2.sdk_base_events.sdk_family",
    "data_by_sdk.pduration_backend_28d_sdk_family": "product_facts_v2.sdk_base_events.pduration_backend_28d_sdk_family",
    "product_facts.total_profile_duration_accepted_28d": "product_facts_v2.organizations_events.total_profile_duration_accepted_28d",
    "product_facts.org_active_frontend": "product_facts_v2.organizations_feature_flags.org_active_frontend",
    "product_facts.sum_indexed_spans_accepted": "product_facts_v2.organizations_events.sum_indexed_spans_accepted",
    "sentry_organizationoptions_new.key": "product_facts_v2.organizations_feature_flags.key",
    "sentry_organizationoptions_new.cohort_number": "product_facts_v2.organizations_feature_flags.cohort_number",
    "product_facts_events.sdk_name": "product_facts_v2.organizations_events.sdk_name",
    "data_by_sdkversion.number_of_events": "product_facts_v2.sdk_base_events.number_of_events",
    "product_facts.indexed_spans_accepted_28d": "product_facts_v2.organizations_events.indexed_spans_accepted_28d",
    "product_facts.seer_issue_scans_accepted_28d": "product_facts_v2.organizations_seer.seer_issue_scans_accepted_28d",
    "product_facts.seer_issue_fixes_accepted_28d": "product_facts_v2.organizations_seer.seer_issue_fixes_accepted_28d",
    "product_facts.logs_count_28d": "product_facts_v2.organizations_events.logs_count_28d",
    "per_product_trials.trial_type": "product_facts_v2.organizations_analytics_summary.trial_type",
    "product_facts.daily_new_issues_28d_org": "product_facts_v2.organizations_issues.daily_new_issues_28d_org",
    "product_facts.error_issue_views_28day_agg": "product_facts_v2.organizations_issues.error_issue_views_28day_agg",
    "product_facts.daily_resolved_issues_28d_org": "product_facts_v2.organizations_issues.daily_resolved_issues_28d_org",
    "product_facts_events.events_28d": "product_facts_v2.organizations_events.events_28d",
    "product_facts.org_team_count": "product_facts_v2.organizations_analytics_summary.org_team_count",
    "product_facts.daily_ignored_issues_28d_org": "product_facts_v2.organizations_issues.daily_ignored_issues_28d_org",
    "product_facts.profile_issue_views_28day_agg": "product_facts_v2.organizations_issues.profile_issue_views_28day_agg",
    "product_facts.performance_issue_views_28day_agg": "product_facts_v2.organizations_issues.performance_issue_views_28day_agg",
    "health_flags.active_flag": "product_facts_v2.organizations_analytics_summary.active_flag",
    "health_flags.engagement_flag": "product_facts_v2.organizations_analytics_summary.engagement_flag",
    "product_facts.generated_issue_alerts_28d": "product_facts_v2.projects_metric_alerts.generated_issue_alerts_28d",
    "product_facts.generated_metric_alerts_28d": "product_facts_v2.projects_metric_alerts.generated_metric_alerts_28d",
    "product_facts.generated_any_alerts_28d": "product_facts_v2.projects_metric_alerts.generated_any_alerts_28d",
    "product_facts.sum_logs_accepted": "product_facts_v2.organizations_events.sum_logs_accepted",
    "data_by_sdkversion.log_size_bytes": "product_facts_v2.sdk_base_events.log_size_bytes",
    "data_by_sdk.log_size_bytes_28d": "product_facts_v2.sdk_base_events.log_size_bytes_28d",
    "data_by_sdkversion.median_log_size_bytes": "product_facts_v2.sdk_base_events.median_log_size_bytes",
    "data_by_sdkversion.logs_origin": "product_facts_v2.sdk_base_events.logs_origin",
    "product_facts.first_logs_date_date": "product_facts_v2.organizations_feature_adoption_dates.first_logs_date_date",
    "product_facts.first_logs_date_month": "product_facts_v2.organizations_feature_adoption_dates.first_logs_date_month",
    "data_by_sdk.events_28d_proj_sdkfamily": "product_facts_v2.sdk_base_events.events_28d_proj_sdkfamily",
    "data_by_sdk.log_size_bytes_28d_proj_sdkfamily": "product_facts_v2.sdk_base_events.log_size_bytes_28d_proj_sdkfamily",
    "trial_view.trial_start_month": "product_facts_v2.organizations_analytics_summary.trial_start_month",
    "top_projects_by_org.is_top_3_project": "product_facts_v2.projects_base_table.is_top_3_project",
    "data_by_project.events_accepted_28d": "product_facts_v2.projects_base_table.events_accepted_28d",
    "data_by_project.proj_seer_issue_scans_accepted_28d": "product_facts_v2.projects_base_table.proj_seer_issue_scans_accepted_28d",
    "product_facts_csm_score.error_spike_score": "product_facts_v2.projects_base_table.error_spike_score",
    "product_facts_csm_score.resolved_issues_score": "product_facts_v2.projects_base_table.resolved_issues_score",
    "product_facts_csm_score.client_side_filtering_score": "product_facts_v2.projects_base_table.client_side_filtering_score",
    "product_facts_csm_score.suspect_commits_score": "product_facts_v2.projects_base_table.suspect_commits_score",
    "product_facts_csm_score.code_mapping_enabled_score": "product_facts_v2.projects_base_table.code_mapping_enabled_score",
    "data_by_project.all_spans_28d": "product_facts_v2.projects_base_table.all_spans_28d",
    "product_facts_csm_score.releases_date_score": "product_facts_v2.projects_base_table.releases_date_score",
    "product_facts_csm_score.alert_uses_environment_filter_score": "product_facts_v2.projects_base_table.alert_uses_environment_filter_score",
    "product_facts_csm_score.data_enrichment_score": "product_facts_v2.projects_base_table.data_enrichment_score",
    "data_by_project.proj_seer_issue_fixes_accepted_28d": "product_facts_v2.projects_base_table.proj_seer_issue_fixes_accepted_28d",
    "product_facts_csm_score.alert_messaging_int_score": "product_facts_v2.projects_base_table.alert_messaging_int_score",
    "product_facts_csm_score.environments_score": "product_facts_v2.projects_base_table.environments_score",
    "product_facts_csm_score.ownership_rules_score": "product_facts_v2.projects_base_table.ownership_rules_score",
    "product_facts_csm_score.alert_errors_count_score": "product_facts_v2.projects_base_table.alert_errors_count_score",
    "product_facts_csm_score.alert_transactional_data_score": "product_facts_v2.projects_base_table.alert_transactional_data_score",
    "product_facts_csm_score.total_weighted_score": "product_facts_v2.projects_base_table.total_weighted_score",
    "product_facts_csm_score.initial_setup_score": "product_facts_v2.projects_base_table.initial_setup_score",
    "product_facts_csm_score.sourcemaps_score": "product_facts_v2.projects_base_table.sourcemaps_score",
    "product_facts_csm_score.breadcrumbs_score": "product_facts_v2.projects_base_table.breadcrumbs_score",
    "product_facts_csm_score.releases_with_commits_score": "product_facts_v2.projects_base_table.releases_with_commits_score",
    "product_facts_csm_score.stacktrace_link_status_score": "product_facts_v2.projects_base_table.stacktrace_link_status_score",
    "product_facts_csm_score.inbound_filters_score": "product_facts_v2.projects_base_table.inbound_filters_score",
    "product_facts_csm_score.dashboards_score": "product_facts_v2.projects_base_table.dashboards_score",
    "data_by_project.all_transactions_28d": "product_facts_v2.projects_base_table.all_transactions_28d",
    "data_by_project.replays_accepted_28d": "product_facts_v2.projects_base_table.replays_accepted_28d",
    "product_facts_csm_score.symbolicated_score": "product_facts_v2.projects_base_table.symbolicated_score",
    "product_facts_csm_score.alert_custom_tags_score": "product_facts_v2.projects_base_table.alert_custom_tags_score",
    "product_facts_csm_score.scm_score": "product_facts_v2.projects_base_table.scm_score",
    "product_facts_csm_score.alert_ticketing_integration_score": "product_facts_v2.projects_base_table.alert_ticketing_integration_score",
    "product_facts_csm_score.team_notification_score": "product_facts_v2.projects_base_table.team_notification_score",
    "product_facts_csm_score.sso_score": "product_facts_v2.projects_base_table.sso_score",
    "product_facts_csm_score.alert_regression_score": "product_facts_v2.projects_base_table.alert_regression_score",
    "product_facts_csm_score.custom_tags_score": "product_facts_v2.projects_base_table.custom_tags_score",
    "data_by_project.logs_count_28d": "product_facts_v2.projects_base_table.logs_count_28d",
    "product_facts_csm_score.alerts_and_dashboards_score": "product_facts_v2.projects_base_table.alerts_and_dashboards_score",
    "product_facts_csm_score.scim_enabled_score": "product_facts_v2.projects_base_table.scim_enabled_score",
    "user_details_for_analytics.email": "product_facts_v2.users_seer.email",
    "product_facts.github_integration": "product_facts_v2.organizations_feature_flags.github_integration",
    "per_product_trials.product_trial_start_date": "product_facts_v2.organizations_analytics_summary.product_trial_start_date",
    "project_uptime_details.auto_detected_monitors": "product_facts_v2.projects_uptime_monitoring.auto_detected_monitors",
    "project_uptime_details.onboarding_monitors": "product_facts_v2.projects_uptime_monitoring.onboarding_monitors",
    "project_uptime_details.manually_created_monitors": "product_facts_v2.projects_uptime_monitoring.manually_created_monitors",
    "organization_uptime_summary.has_active_crons_and_manual_uptime_alerts": "product_facts_v2.organizations_uptime_monitoring.has_active_crons_and_manual_uptime_alerts",
    "data_by_sdk.mobile_replays_28d_proj_sdkfamily": "product_facts_v2.sdk_base_events.mobile_replays_28d_proj_sdkfamily",
    "data_by_sdk.total_pduration__28d_sdk_family": "product_facts_v2.sdk_base_events.total_pduration__28d_sdk_family",
    "data_by_sdk.replays_28d_proj_sdkfamily": "product_facts_v2.sdk_base_events.replays_28d_proj_sdkfamily",
    "data_by_sdk.total_accepted_replay_count": "product_facts_v2.sdk_base_events.total_accepted_replay_count",
    "data_by_sdk.number_of_mobile_replays_28d": "product_facts_v2.sdk_base_events.number_of_mobile_replays_28d",
    "data_by_sdk.Total_replays_28d": "product_facts_v2.sdk_base_events.Total_replays_28d",
    "trial_view.next_invoice_channel": "product_facts_v2.organizations_analytics_summary.next_invoice_channel",
    "product_facts.dt_day_of_week": "product_facts_v2.organizations_age_tracking.dt_day_of_week",
    "trial_view.next_edition": "product_facts_v2.organizations_analytics_summary.next_edition",
    "product_facts.org_users_total": "product_facts_v2.organizations_analytics_summary.org_users_total",
    "product_facts.organization_name": "product_facts_v2.organizations_analytics_summary.organization_name",
    "product_facts.crons_monitors_28d": "product_facts_v2.organizations_cron_monitoring.crons_monitors_28d",
    "product_facts_events.active_sdk": "product_facts_v2.organizations_events.active_sdk",
    "trial_view.trial_start_date": "product_facts_v2.organizations_analytics_summary.trial_start_date",
    "billing_model.sum_total_new_arr": "financial_data.daily_arr_by_category.sum_total_new_arr",
    "billing_model.sum_total_expansion_arr": "financial_data.daily_arr_by_category.sum_total_expansion_arr",
    "product_facts.crons_active_monitor_28d": "product_facts_v2.organizations_cron_monitoring.crons_active_monitor_28d",
    "product_facts.crons_checkins_daily": "product_facts_v2.organizations_cron_monitoring.crons_checkins_daily",
    "data_by_project.project_first_event_date_date": "product_facts_v2.projects_base_table.project_first_event_date_date",
    "data_by_project.errors_accepted_28d": "product_facts_v2.projects_base_table.errors_accepted_28d",
    "data_by_project.transactions_accepted_28d": "product_facts_v2.projects_base_table.transactions_accepted_28d",
    "data_by_project.proj_seer_issue_fixes_accepted": "product_facts_v2.projects_base_table.proj_seer_issue_fixes_accepted",
    "product_facts_integrations_array.individual_integration_features": "product_facts_v2.organizations_integrations.individual_integration_features",
    "data_by_project.proj_seer_issue_scans_accepted": "product_facts_v2.projects_base_table.proj_seer_issue_scans_accepted",
    "user_details_for_analytics.full_name": "product_facts_v2.users_seer.full_name",
    "product_facts_autofix_llm.autofix_runs_sum": "product_facts_v2.organizations_autofix_usage.autofix_runs_sum",
    "data_by_sdkversion.errors": "product_facts_v2.sdk_base_events.errors",
    "data_by_project.proj_replays_accepted": "product_facts_v2.projects_base_table.proj_replays_accepted",
    "data_by_sdkversion.sdk_version_replays_support_proj": "product_facts_v2.sdk_base_events.sdk_version_replays_support_proj",
    "sdk_mapping_minversion_project.transaction_support": "product_facts_v2.sdk_org_events.transaction_support",
    "data_by_sdkversion.sdk_version_crons_support_proj": "product_facts_v2.sdk_base_events.sdk_version_crons_support_proj",
    "sdk_mapping_minversion_project.profile_support": "product_facts_v2.sdk_org_events.profile_support",
    "data_by_sdkversion.uses_latest_sdk_version": "product_facts_v2.sdk_base_events.uses_latest_sdk_version",
    "data_by_sdkversion.profiles": "product_facts_v2.sdk_base_events.profiles",
    "data_by_sdkversion.sdk_version": "product_facts_v2.sdk_base_events.sdk_version",
    "data_by_sdkversion.sdk_version_profiles_support_proj": "product_facts_v2.sdk_base_events.sdk_version_profiles_support_proj",
    "sdk_mapping_minversion_project.crons_support": "product_facts_v2.sdk_org_events.crons_support",
    "data_by_sdk.sdk_integrations_string": "product_facts_v2.sdk_base_events.sdk_integrations_string",
    "data_by_sdkversion.transactions": "product_facts_v2.sdk_base_events.transactions",
    "sdk_mapping_minversion_project.replay_support": "product_facts_v2.sdk_org_events.replay_support",
    "data_by_project.proj_spans_accepted": "product_facts_v2.projects_base_table.proj_spans_accepted",
    "data_by_sdkversion.sdk_version_transactions_support_proj": "product_facts_v2.sdk_base_events.sdk_version_transactions_support_proj",
    "data_by_project.profiles_accepted_28d": "product_facts_v2.projects_base_table.profiles_accepted_28d",
    "data_by_project.attachments_accepted_28d": "product_facts_v2.projects_base_table.attachments_accepted_28d",
    "data_by_project.spike_protection_disabled": "product_facts_v2.projects_base_table.spike_protection_disabled",
    "data_by_project.regression_issue_alerts_count": "product_facts_v2.projects_base_table.regression_issue_alerts_count",
    "data_by_project.sdk_integrations_enabled": "product_facts_v2.projects_base_table.sdk_integrations_enabled",
    "data_by_project.new_issue_alerts_count": "product_facts_v2.projects_base_table.new_issue_alerts_count",
    "data_by_project.single_event_issue_percent_28d_proj": "product_facts_v2.projects_base_table.single_event_issue_percent_28d_proj",
    "data_by_project.messaging_integration_issue_alerts_count": "product_facts_v2.projects_base_table.messaging_integration_issue_alerts_count",
    "data_by_project.proj_team_member_count": "product_facts_v2.projects_base_table.proj_team_member_count",
    "sdk_integrations_array.individual_sdk_integrations": "product_facts_v2.sdk_org_events.individual_sdk_integrations",
    "data_by_sdk.sessions_crash_free_sum": "product_facts_v2.sdk_base_events.sessions_crash_free_sum",
    "data_by_project.environment_based_metric_alerts_count": "product_facts_v2.projects_base_table.environment_based_metric_alerts_count",
    "data_by_project.client_side_sampling_used": "product_facts_v2.projects_base_table.client_side_sampling_used",
    "data_by_project.server_side_filters_used": "product_facts_v2.projects_base_table.server_side_filters_used",
    "data_by_project.error_spike_metric_alerts_count": "product_facts_v2.projects_base_table.error_spike_metric_alerts_count",
    "data_by_project.client_side_filters_used": "product_facts_v2.projects_base_table.client_side_filters_used",
    "data_by_project.error_count_issue_alerts_count": "product_facts_v2.projects_base_table.error_count_issue_alerts_count",
    "data_by_project.transactional_data_metric_alerts_count": "product_facts_v2.projects_base_table.transactional_data_metric_alerts_count",
    "data_by_project.custom_tags_issue_alerts_count": "product_facts_v2.projects_base_table.custom_tags_issue_alerts_count",
    "data_by_project.team_notification_issue_alert_count": "product_facts_v2.projects_base_table.team_notification_issue_alert_count",
    "product_facts.sso_provider": "product_facts_v2.organizations_sso_configuration.sso_provider",
    "product_facts.sso_users_28d": "product_facts_v2.organizations_sso_configuration.sso_users_28d",
    "product_facts.external_ticket_integration_flag": "product_facts_v2.organizations_feature_flags.external_ticket_integration_flag",
    "product_facts.sso_status": "product_facts_v2.organizations_sso_configuration.sso_status",
    "data_by_project.errors_over_quota_quarter_count": "product_facts_v2.projects_base_table.errors_over_quota_quarter_count",
    "data_by_project.transactions_over_quota_quarter_count": "product_facts_v2.projects_base_table.transactions_over_quota_quarter_count",
    "data_by_project.spend_allocation_enabled_flag": "product_facts_v2.projects_base_table.spend_allocation_enabled_flag",
    "data_by_project.replays_over_quota_quarter_count": "product_facts_v2.projects_base_table.replays_over_quota_quarter_count",
    "data_by_project.create_jira_ticket_issue_alert_count": "product_facts_v2.projects_base_table.create_jira_ticket_issue_alert_count",
    "data_by_project.daily_resolved_issues_28d_proj": "product_facts_v2.projects_base_table.daily_resolved_issues_28d_proj",
    "data_by_project.codemapping_enabled": "product_facts_v2.projects_base_table.codemapping_enabled",
    "data_by_project.daily_new_issues_28d_proj": "product_facts_v2.projects_base_table.daily_new_issues_28d_proj",
    "data_by_project.releases_created_through_cli": "product_facts_v2.projects_base_table.releases_created_through_cli",
    "data_by_project.releases_having_commits_associated": "product_facts_v2.projects_base_table.releases_having_commits_associated",
    "data_by_project.ownership_rules": "product_facts_v2.projects_base_table.ownership_rules",
    "data_by_project.daily_ignored_issues_28d_proj": "product_facts_v2.projects_base_table.daily_ignored_issues_28d_proj",
    "user_details_for_analytics.is_superuser": "product_facts_v2.users_seer.is_superuser",
    "user_details_for_analytics.is_staff": "product_facts_v2.users_seer.is_staff",
    "product_facts.performance_units_accepted_28d": "product_facts_v2.organizations_data_outcomes.performance_units_accepted_28d",
    "product_facts.transactions_utilization_rate_avg": "product_facts_v2.organizations_data_outcomes.transactions_utilization_rate_avg",
    "product_facts.org_active_traces": "product_facts_v2.organizations_events.org_active_traces",
    "product_facts.spans_utilization_rate_avg": "product_facts_v2.organizations_events.spans_utilization_rate_avg",
    "accounts_billingmetricusage_on_org.sum_quantity_28d": "financial_data.accounts_billingmetricusage_on_org.sum_quantity_28d",
    "product_facts.first_spans_date_week": "product_facts_v2.organizations_feature_adoption_dates.first_spans_date_week",
    "product_facts.first_spans_date_date": "product_facts_v2.organizations_feature_adoption_dates.first_spans_date_date",
    "data_by_sdk.spans_28d_proj_sdkfamily": "product_facts_v2.sdk_base_events.spans_28d_proj_sdkfamily",
    "data_by_sdk.transactions_28d_proj_sdkfamily": "product_facts_v2.sdk_base_events.transactions_28d_proj_sdkfamily",
    "product_facts.profiles_accepted_28d": "product_facts_v2.organizations_data_outcomes.profiles_accepted_28d",
    "product_facts.sum_errors_rate_limited": "product_facts_v2.organizations_data_outcomes.sum_errors_rate_limited",
    "expansion_churn_scores.slug": "financial_data.daily_arr_combined.slug",
    "product_facts.daily_new_performance_issues_28d_org": "product_facts_v2.organizations_issues.daily_new_performance_issues_28d_org",
    "product_facts.sum_indexed_transactions_rate_limited": "product_facts_v2.organizations_data_outcomes.sum_indexed_transactions_rate_limited",
    "product_facts.indexed_transactions_accepted_28d": "product_facts_v2.organizations_data_outcomes.indexed_transactions_accepted_28d",
    "product_facts.sum_indexed_transactions_filtered": "product_facts_v2.organizations_data_outcomes.sum_indexed_transactions_filtered",
    "product_facts.sum_errors_filtered": "product_facts_v2.organizations_data_outcomes.sum_errors_filtered",
    "data_by_sdk.number_of_errors_28d": "product_facts_v2.sdk_base_events.number_of_errors_28d",
    "data_by_sdk.number_of_transactions_28d": "product_facts_v2.sdk_base_events.number_of_transactions_28d",
    "data_by_project.indexed_transactions_accepted_28d": "product_facts_v2.projects_base_table.indexed_transactions_accepted_28d",
    "data_by_sdk.number_of_events_28d": "product_facts_v2.sdk_base_events.number_of_events_28d",
    "product_facts_alerts_array.primary_alertrule_id_slack": "product_facts_v2.projects_metric_alerts.primary_alertrule_id_slack",
    "product_facts_alerts_array.primary_alertrule_id_email": "product_facts_v2.projects_metric_alerts.primary_alertrule_id_email",
    "product_facts_alerts_array.primary_alertrule_id_pagerduty": "product_facts_v2.projects_metric_alerts.primary_alertrule_id_pagerduty",
    "product_facts_alerts_array.primary_alertrule_id": "product_facts_v2.projects_metric_alerts.primary_alertrule_id",
    "product_facts_alerts_array.primary_alertrule_id_msteams": "product_facts_v2.projects_metric_alerts.primary_alertrule_id_msteams",
    "product_facts_alerts_array.primary_alertrule_id_sentryapp": "product_facts_v2.projects_metric_alerts.primary_alertrule_id_sentryapp",
    "product_facts_alerts_array.primary_alertrule_id_active": "product_facts_v2.projects_metric_alerts.primary_alertrule_id_active",
    "product_facts.github_stacktrace_linked_successes": "product_facts_v2.organizations_events.github_stacktrace_linked_successes",
    "product_facts.created_dashboard_count": "product_facts_v2.organizations_dashboards.created_dashboard_count",
    "product_facts.transaction_summary_visit_count": "product_facts_v2.organizations_dashboards.transaction_summary_visit_count",
    "product_facts.performance_landing_page_visit_count": "product_facts_v2.organizations_dashboards.performance_landing_page_visit_count",
    "product_facts.opened_discover_query_count": "product_facts_v2.organizations_feature_flags.opened_discover_query_count",
    "product_facts.viewed_dashboard_count": "product_facts_v2.organizations_dashboards.viewed_dashboard_count",
    "data_by_project.proj_codeowner_rule_count": "product_facts_v2.projects_base_table.proj_codeowner_rule_count",
    "product_facts_analytics_array.percent_of_users_visiting_discover_query": "product_facts_v2.projects_analytics_summary.percent_of_users_visiting_discover_query",
    "product_facts.avg_org_active_users_28d": "product_facts_v2.organizations_age_tracking.avg_org_active_users_28d",
    "data_by_sdk.number_of_events": "product_facts_v2.sdk_base_events.number_of_events",
    "product_facts.sum_transactions_rate_limited": "product_facts_v2.organizations_data_outcomes.sum_transactions_rate_limited",
    "product_facts.sum_transactions_accepted": "product_facts_v2.organizations_data_outcomes.sum_transactions_accepted",
    "product_facts.total_metric_alerts_generated": "product_facts_v2.projects_metric_alerts.total_metric_alerts_generated",
    "product_facts.total_issue_alerts_generated": "product_facts_v2.projects_metric_alerts.total_issue_alerts_generated",
    "issues_by_type_struct.daily_new_performance_issues_sum": "product_facts_v2.issues_org_type.daily_new_performance_issues_sum",
    "issues_by_type_struct.daily_resolved_performance_issues_sum": "product_facts_v2.issues_org_type.daily_resolved_performance_issues_sum",
    "product_facts.sum_replays_rate_limited": "product_facts_v2.organizations_data_outcomes.sum_replays_rate_limited",
    "product_facts_features_array._individual_features": "product_facts_v2.organizations_feature_flags._individual_features",
    "product_facts_events.web": "product_facts_v2.organizations_events.web",
    "product_facts_events.server": "product_facts_v2.organizations_events.server",
    "product_facts_events.desktop": "product_facts_v2.organizations_events.desktop",
    "product_facts_events.mobile": "product_facts_v2.organizations_events.mobile",
    "product_facts.sum_errors_invalid_abuse": "product_facts_v2.organizations_data_outcomes.sum_errors_invalid_abuse",
    "product_facts.sum_transactions_filtered": "product_facts_v2.organizations_data_outcomes.sum_transactions_filtered",
    "product_facts.sum_transactions_invalid_abuse": "product_facts_v2.organizations_data_outcomes.sum_transactions_invalid_abuse",
    "product_facts.discover_activity_count": "product_facts_v2.organizations_feature_flags.discover_activity_count",
    "data_by_sdk.sessions_crashed_28d": "product_facts_v2.sdk_base_events.sessions_crashed_28d",
    "data_by_sdk.sessions_abnormal_sum": "product_facts_v2.sdk_base_events.sessions_abnormal_sum",
    "data_by_sdk.total_sessions_28d": "product_facts_v2.sdk_base_events.total_sessions_28d",
    "data_by_sdk.crash_free_sessions_28d": "product_facts_v2.sdk_base_events.crash_free_sessions_28d",
    "data_by_sdk.sessions_crashed_sum": "product_facts_v2.sdk_base_events.sessions_crashed_sum",
    "data_by_sdk.crash_free_rate_percent": "product_facts_v2.sdk_base_events.crash_free_rate_percent",
    "data_by_sdk.crash_free_rate": "product_facts_v2.sdk_base_events.crash_free_rate",
    "data_by_sdk.total_sessions_sum": "product_facts_v2.sdk_base_events.total_sessions_sum",
    "product_facts.org_id_link_to_engagement_score": "product_facts_v2.organizations_feature_flags.org_id_link_to_engagement_score",
    "product_facts.org_id_link_to_project_health": "product_facts_v2.organizations_feature_flags.org_id_link_to_project_health",
    "product_facts.sum_attachments_accepted": "product_facts_v2.organizations_data_outcomes.sum_attachments_accepted",
    "product_facts.sum_profiles_accepted": "product_facts_v2.organizations_data_outcomes.sum_profiles_accepted",
    "product_facts.org_active_mobile": "product_facts_v2.organizations_feature_flags.org_active_mobile",
    "product_facts.alert_rules": "product_facts_v2.organizations_feature_flags.alert_rules",
    "product_facts.source_maps": "product_facts_v2.organizations_feature_flags.source_maps",
    "product_facts.release_tracking": "product_facts_v2.organizations_feature_flags.release_tracking",
    "product_facts.custom_tags": "product_facts_v2.organizations_feature_flags.custom_tags",
    "product_facts.transactions_client_side_sampling_rate_28d": "product_facts_v2.organizations_analytics_summary.transactions_client_side_sampling_rate_28d",
    "product_facts.seer_issue_fixes_accepted_sum": "product_facts_v2.organizations_seer.seer_issue_fixes_accepted_sum",
    "product_facts.seer_issue_scans_accepted_sum": "product_facts_v2.organizations_seer.seer_issue_scans_accepted_sum",
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
                # Skip tiles not on the old product_facts explore
                if q.model != "super_big_facts" or q.view != OLD_EXPLORE:
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
