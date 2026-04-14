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
OLD_EXPLORE  = "product_facts"
NEW_MODEL    = "super_big_facts"
NEW_EXPLORE  = "product_usage_org_proj"
NEW_EXPLORE_2 = "product_usage_sdk"

# ─────────────────────────────────────────────
# VIEWS joined into NEW_EXPLORE
# Any field from a view NOT in this list will be flagged by --validate
# Add views here as you confirm they exist in product_usage_org_proj
# ─────────────────────────────────────────────
JOINED_VIEWS_IN_NEW_EXPLORE = {
    "accounts_billingmetricusage_on_org",
    "daily_arr_by_category",
    "daily_arr_combined",
    "daily_financial_data_billing_category",
    "daily_financial_data_billing_category_struct",
    "engagement_score",
    "issues_org_type",
    "organizations",
    "organizations_age_tracking",
    "organizations_analytics_summary",
    "organizations_autofix_usage",
    "organizations_cron_monitoring",
    "organizations_dashboards",
    "organizations_data_outcomes",
    "organizations_emerge",
    "organizations_events",
    "organizations_feature_adoption_dates",
    "organizations_feature_flags",
    "organizations_feedback_received",
    "organizations_integrations",
    "organizations_issues",
    "organizations_promocode_usage",
    "organizations_seer",
    "organizations_sso_configuration",
    "organizations_uptime_monitoring",
    "product_facts_v2_base",
    "projects_analytics_summary",
    "projects_base_table",
    "projects_data_outcomes",
    "projects_emerge",
    "projects_metric_alerts",
    "projects_uptime_monitoring",
    "sdk_base_events",
    "sdk_org_events",
    "subscriptions_v3",
    "users_seer",
}

# ─────────────────────────────────────────────
# FIELD MAPS — keyed by (old_explore, new_explore)
# Fields from joined views that haven't changed don't need to be listed here
# ─────────────────────────────────────────────
FIELD_MAPS = {
("product_facts", "product_usage_org_proj"): {
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
    "data_by_project.proj_trace_metric_items_accepted": "projects_data_outcomes.trace_metric_items_accepted",
    # TODO — find new field name for this before migrating dashboards that use it:
    # "project_uptime_details.total_active_monitors":           "???",
    # === additional mappings ===
    "product_facts.dt_week": "product_facts_v2_base.dt_week",
    "new_seer_orgs.estimated_seats": "organizations_seer.estimated_seats",
    "product_facts_org_seer_user_rollup.billable_seats_28d": "users_seer.billable_seats_28d",
    "org_feedback_received.total_comments": "organizations_feedback_received.total_comments",
    "org_feedback_received.total_fixes_applied": "organizations_feedback_received.total_fixes_applied",
    "org_feedback_received.total_fixes_rejected": "organizations_feedback_received.total_fixes_rejected",
    "org_feedback_received.total_downvotes": "organizations_feedback_received.total_downvotes",
    "org_feedback_received.total_upvotes": "organizations_feedback_received.total_upvotes",
    "org_feedback_received.total_hearts": "organizations_feedback_received.total_hearts",
    "subscriptions_v3.total_churn_arr": "daily_arr_changes_org.sum_total_churn_arr",
    "product_facts.orgs_issue_details_viewed_has_root_cause_true": "organizations_seer.orgs_issue_details_viewed_has_root_cause_true",
    "product_facts.orgs_found_solution": "organizations_seer.orgs_found_solution",
    "product_facts.orgs_autofix_pr_merged": "organizations_autofix_usage.sum_autofix_merged_prs",
    "new_seer_orgs.is_new_seer_org": "organizations_seer.is_new_seer_org",
    "new_seer_orgs.seer_enabled_week": "organizations_seer.seer_enabled_week",
    "new_seer_orgs.total_invoiced_seats": "organizations_seer.total_invoiced_seats",
    "new_seer_orgs.seer_churn_date": "organizations_seer.seer_churn_date",
    "new_seer_orgs.seer_enabled_date": "organizations_seer.seer_enabled_date",
    "new_seer_orgs.seats_at_churn": "organizations_seer.seats_at_churn",
    "product_facts_org_seer.total_events_sum": "organizations_seer.total_events_sum",
    "product_facts_org_seer.total_unique_prs_sum": "organizations_seer.total_unique_prs_sum",
    "product_facts_org_seer.total_unique_repos_sum": "organizations_seer.total_unique_repos_sum",
    "new_seer_orgs.seer_enabled_month": "organizations_seer.seer_enabled_month",
    "org_feedback_received.prs_with_feedback": "organizations_feedback_received.prs_with_feedback",
    "product_facts_org_seer.total_unique_repos": "organizations_seer.total_unique_repos",
    "product_facts.seer_cohort": "organizations_seer.seer_cohort",
    "product_facts.seer_filter_mode": "organizations_seer.seer_filter_mode",
    "product_facts.seer_segment_match": "organizations_seer.seer_segment_match",
    "product_facts_org_seer_user_rollup.is_billable_any_seat_28d": "users_seer.is_billable_any_seat_28d",
    "product_facts.seer_billable_seats_28d": "organizations_seer.seer_billable_seats_28d",
    "seer_usage_cost.total_dollar_cost": "organizations_seer.total_dollar_cost",
    "seer_usage_cost.step": "organizations_seer.step",
    "seer_usage_cost.feature": "organizations_seer.feature",
    "seer_pr_events.distinct_prs": "organizations_seer.distinct_prs",
    "product_facts.trace_metric_items_accepted_28d": "organizations_data_outcomes.trace_metric_items_accepted_28d",
    "metric_type_events.metric_items": "projects_data_outcomes.metrics_accepted",
    "data_by_sdk.trace_metric_size_bytes_28d": "sdk_base_events.trace_metric_size_bytes_28d_proj_sdk",
    "data_by_sdkversion.trace_metric_items": "sdk_base_events.trace_metric_items",
    "data_by_sdk.trace_metric_items_28d": "sdk_base_events.trace_metric_items_28d_proj_sdk",
    "data_by_sdk.sdk_name": "sdk_base_events.sdk_name",
    "data_by_sdkversion.trace_metric_size_bytes": "sdk_base_events.trace_metric_size_bytes",
    "data_by_sdkversion.median_metric_size_bytes": "sdk_base_events.median_trace_metric_size_bytes",
    "metric_type_events.median_attributes": "projects_data_outcomes.median_attributes",
    "product_facts.org_ea_flag": "organizations_analytics_summary.org_ea_flag",
    "product_facts.is_last_day_of_fiscal_quarter": "product_facts_v2_base.is_last_day_of_fiscal_quarter",
    "product_facts.dt_fiscal_quarter": "product_facts_v2_base.dt_fiscal_quarter",
    "product_facts_promocodeclaimant.date_added_month": "organizations_promocode_usage__promo_code_struct.date_added_month",
    "product_facts_promocodeclaimant.date_added_date": "organizations_promocode_usage__promo_code_struct.date_added_date",
    "product_facts_promocodeclaimant.promocode": "organizations_promocode_usage__promo_code_struct.promocode",
    "product_facts_promocodeclaimant.date_added_week": "organizations_promocode_usage__promo_code_struct.date_added_week",
    "product_facts_promocodeclaimant.promocode_id": "organizations_promocode_usage__promo_code_struct.promocode_id",
    "subscriptions_v3.total_new_arr": "daily_arr_changes_org.sum_total_new_arr",
    "subscriptions_v3.total_expansion_arr": "daily_arr_changes_org.sum_total_expansion_arr",
    "product_facts.replays_accepted_28d": "projects_data_outcomes.replays_accepted_28d",
    "product_facts.errors_accepted_28d": "projects_data_outcomes.errors_accepted_28d",
    "product_facts.transactions_accepted_28d": "projects_data_outcomes.transactions_accepted_28d",
    "product_facts.replays_utilization_rate": "projects_data_outcomes.replays_utilization_rate",
    "product_facts.spans_utilization_rate": "projects_data_outcomes.spans_utilization_rate",
    "product_facts.transactions_utilization_rate": "projects_data_outcomes.transactions_utilization_rate",
    "product_facts.errors_utilization_rate": "projects_data_outcomes.errors_utilization_rate",
    "product_facts.logs_accepted_28d": "projects_data_outcomes.logs_bytes_accepted_28d",
    "product_facts.org_active_users_28d": "organizations_active_users.org_active_users_28d",
    "product_facts.spans_accepted_28d": "organizations_data_outcomes.spans_accepted_28d",
    "billing_model.sum_total_churn_arr": "daily_arr_changes_org.sum_total_churn_arr",
    "product_facts_emerge.size_builds_28d": "organizations_emerge.size_builds_28d",
    "product_facts_emerge.size_builds_total": "projects_emerge.size_builds_total",
    "emerge_project.size_builds_total": "projects_emerge.size_builds_total",
    "emerge_project.size_builds_28d": "projects_emerge.size_builds_28d",
    "data_by_project.project_platform": "sentry_project.platform",
    "product_facts.sum_org_users_total": "organizations_active_users.org_users_total",
    "product_facts_emerge.distribution_builds_28d": "organizations_emerge.distribution_builds_28d",
    "product_facts_emerge.distribution_builds_total": "organizations_emerge.distribution_builds_total",
    "product_facts_emerge.distribution_installs_total": "organizations_emerge.distribution_installs_total",
    "product_facts_emerge.distribution_installs_28d": "projects_emerge.distribution_installs_28d",
    "emerge_first_adoption_dates.first_size_analysis_date_week": "organizations_feature_adoption_dates.first_size_analysis_date",
    "emerge_first_adoption_dates.first_size_analysis_date_date": "organizations_feature_adoption_dates.first_size_analysis_date",
    "emerge_first_adoption_dates.first_distribution_build_date_week": "organizations_feature_adoption_dates.first_distribution_builds_week",
    "emerge_first_adoption_dates.first_distribution_build_date_date": "organizations_feature_adoption_dates.first_distribution_builds_date",
    "product_facts.crons_active_monitor_1d": "organizations_cron_monitoring.crons_active_monitor_1d",
    "product_facts_events.sdk_family": "organizations_events.sdk_family",
    "product_facts.performance_units_accepted": "projects_data_outcomes.performance_units_accepted",
    "product_facts_events.event_type": "organizations_events.event_type",
    "accounts_billingmetricusage_on_org.outcome_readable": "accounts_billingmetricusage_on_org.outcome_readable",
    "organization_uptime_summary.has_active_crons_and_manual_uptime_alerts": "organizations_uptime_monitoring.has_active_crons_and_manual_uptime_alerts",
    "data_by_sdkversion.total_profile_duration_continuous": "sdk_base_events.profile_duration_backend",
    "data_by_sdkversion.profile_duration_frontend": "sdk_base_events.profile_duration_frontend",
    "data_by_sdkversion.profile_duration_backend": "sdk_base_events.profile_duration_backend",
    "product_facts.combined_integration_features": "organizations_integrations.all_integration_features_string",
    "data_by_project.project_name": "sentry_projects.name",
    "data_by_project.project_id": "product_facts_v2_base.project_id",
    "data_by_project.project_count": "projects_data_outcomes.project_count",
    "data_by_project.proj_errors_accepted": "projects_data_outcomes.errors_accepted",
    "data_by_project.has_environments_current_enabled": "projects_configuration.has_environments_current_flag",
    "data_by_project.primary_sdk": "projects_base_table.primary_sdk",
    "product_facts.org_active_backend": "organizations_feature_flags.org_active_backend",
    "product_facts_events.number_of_events_sum": "organizations_events.number_of_events_sum",
    "billing_model_billing_category.sum_contraction_arr": "daily_arr_changes_category.sum_total_contraction_arr",
    "billing_model_billing_category.sum_reactivation_arr": "daily_arr_changes_category.sum_total_reactivation_arr",
    "daily_financial_data_billing_category_struct.change_in_ondemand_arr": "daily_arr_changes_category.ondemand_change_arr",
    "billing_model_billing_category.sum_churn_arr": "daily_arr_changes_category.sum_total_churn_arr",
    "billing_model_billing_category.sum_new_arr": "daily_arr_changes_category.sum_total_new_arr",
    "billing_model.billing_model": "daily_arr_by_category.billing_model",
    "billing_model_billing_category.sum_expansion_arr": "daily_arr_changes_category.sum_total_expansion_arr",
    "product_facts.first_continuous_profile_date_date": "organizations_feature_adoption_dates.first_continuous_profile_date",
    "product_facts.profile_duration_accepted_28d": "organizations_data_outcomes.profile_duration_accepted_28d",
    "product_facts.frontend_profile_duration_accepted_28d": "projects_data_outcomes.frontend_profile_duration_accepted_28d",
    "product_facts.first_ui_profile_date_date": "organizations_feature_adoption_dates.first_ui_profile_date_date",
    "product_facts.front_end_profile_duration_accepted_sum": "projects_data_outcomes.frontend_profile_duration_accepted",
    "product_facts.profile_duration_accepted_sum": "projects_data_outcomes.profile_duration_accepted",
    "product_facts.total_profile_duration_accepted_sum": "organizations_data_outcomes.profile_duration_accepted",
    "data_by_sdk.pduration_frontend_28d_sdk_family": "sdk_base_events.pduration_frontend_28d_proj_sdkfamily",
    "data_by_sdk.sdk_family": "sdk_map_new.language_family",
    "data_by_sdk.pduration_backend_28d_sdk_family": "sdk_base_events.pduration_backend_28d_proj_sdkfamily",
    "product_facts.total_profile_duration_accepted_28d": "projects_data_outcomes.profile_duration_accepted_28d",
    "product_facts.org_active_frontend": "sdk_map_new.framework_frontend",
    "product_facts.sum_indexed_spans_accepted": "projects_data_outcomes.indexed_spans_accepted",
    "product_facts_events.sdk_name": "organizations_events.sdk_name",
    "data_by_sdkversion.number_of_events": "sdk_base_events.events",
    "product_facts.indexed_spans_accepted_28d": "projects_data_outcomes.indexed_spans_accepted_28d",
    "product_facts.seer_issue_scans_accepted_28d": "organizations_data_outcomes.seer_issue_scans_accepted_28d",
    "product_facts.seer_issue_fixes_accepted_28d": "organizations_data_outcomes.seer_issue_fixes_accepted_28d",
    "product_facts.logs_count_28d": "projects_data_outcomes.logs_items_accepted_28d",
    "per_product_trials.trial_type": "organizations_per_product_trials.trial_type",
    "product_facts.daily_new_issues_28d_org": "organizations_issues.daily_new_issues_28d_org",
    "product_facts.error_issue_views_28day_agg": "organizations_analytics_specific.error_issue_views_28d_org",
    "product_facts.daily_resolved_issues_28d_org": "projects_issues.daily_resolved_issues_28d_proj",
    "product_facts_events.events_28d": "organizations_data_outcomes.events_accepted_28d",
    "product_facts.org_team_count": "organizations_analytics_summary.org_team_count",
    "product_facts.daily_ignored_issues_28d_org": "projects_issues.daily_ignored_issues_28d_proj",
    "product_facts.profile_issue_views_28day_agg": "projects_analytics_specific.profile_issue_views_28d_proj",
    "product_facts.performance_issue_views_28day_agg": "projects_analytics_specific.performance_issue_views_28d_proj",
    "health_flags.active_flag": "organizations_analytics_summary.active_flag",
    "health_flags.engagement_flag": "organizations_analytics_summary.engagement_flag",
    "product_facts.generated_issue_alerts_28d": "projects_analytics_specific.generated_issue_alerts_28d_proj",
    "product_facts.generated_metric_alerts_28d": "projects_analytics_specific.generated_metric_alerts_28d_proj",
    "product_facts.generated_any_alerts_28d": "projects_analytics_specific.generated_total_alerts_28d_proj",
    "product_facts.sum_logs_accepted": "projects_data_outcomes.logs_bytes_accepted",
    "data_by_sdkversion.log_size_bytes": "sdk_base_events.log_size_bytes",
    "data_by_sdk.log_size_bytes_28d": "sdk_base_events.log_size_bytes_28d_proj_sdk",
    "data_by_sdkversion.median_log_size_bytes": "sdk_base_events.median_log_size_bytes",
    "data_by_sdkversion.logs_origin": "sdk_base_events.logs_origin",
    "product_facts.first_logs_date_date": "organizations_feature_adoption_dates.first_logs_date_date",
    "product_facts.first_logs_date_month": "organizations_feature_adoption_dates.first_logs_date_month",
    "data_by_sdk.events_28d_proj_sdkfamily": "sdk_base_events.events_28d_proj_sdkfamily",
    "data_by_sdk.log_size_bytes_28d_proj_sdkfamily": "sdk_base_events.log_size_bytes_28d_proj_sdkfamily",
    "trial_view.trial_start_month": "organizations_trial_view.trial_start_month",
    "top_projects_by_org.is_top_3_project": "projects_base_table.is_top_3_project",
    "data_by_project.events_accepted_28d": "projects_data_outcomes.events_accepted_28d",
    "data_by_project.proj_seer_issue_scans_accepted_28d": "projects_data_outcomes.seer_issue_scans_accepted_28d",
    "data_by_project.all_spans_28d": "projects_data_outcomes.all_spans_28d",
    "data_by_project.proj_seer_issue_fixes_accepted_28d": "projects_data_outcomes.seer_issue_fixes_accepted_28d",
    "data_by_project.all_transactions_28d": "projects_data_outcomes.all_transactions_28d",
    "data_by_project.replays_accepted_28d": "projects_data_outcomes.replays_accepted_28d",
    "data_by_project.logs_count_28d": "projects_data_outcomes.logs_items_accepted_28d",
    "product_facts.github_integration": "organizations_integrations.github_integration",
    "per_product_trials.product_trial_start_date": "organizations_per_product_trials.product_trial_start_date",
    "project_uptime_details.auto_detected_monitors": "projects_uptime_monitoring.auto_detected_monitors",
    "project_uptime_details.onboarding_monitors": "projects_uptime_monitoring.sum_onboarding_monitors",
    "project_uptime_details.manually_created_monitors": "projects_uptime_monitoring.manually_created_monitors",
    "data_by_sdk.mobile_replays_28d_proj_sdkfamily": "sdk_base_events.mobile_replays_28d_proj_sdkfamily",
    "data_by_sdk.total_pduration__28d_sdk_family": "sdk_base_events.total_pduration__28d_sdk_family",
    "data_by_sdk.replays_28d_proj_sdkfamily": "sdk_base_events.replays_28d_proj_sdkfamily",
    "data_by_sdk.total_accepted_replay_count": "sdk_base_events.replays_28d_proj_sdk",
    "data_by_sdk.number_of_mobile_replays_28d": "sdk_base_events.mobile_replays_28d_proj_sdk",
    "data_by_sdk.Total_replays_28d": "sdk_base_events.replays_28d_proj_sdk",
    "trial_view.next_invoice_channel": "organizations_trial_view.next_invoice_channel",
    "product_facts.dt_day_of_week": "product_facts_v2_base.dt_day_of_week",
    "trial_view.next_edition": "organizations_trial_view.next_edition",
    "product_facts.org_users_total": "organizations_active_users.org_users_total",
    "product_facts.organization_name": "organizations_analytics_summary.organization_name",
    "product_facts.crons_monitors_28d": "organizations_cron_monitoring.crons_monitors_28d",
    "product_facts_events.active_sdk": "organizations_events.active_sdk",
    "trial_view.trial_start_date": "organizations_trial_view.trial_start_date",
    "billing_model.sum_total_new_arr": "daily_arr_changes_org.sum_total_new_arr",
    "billing_model.sum_total_expansion_arr": "daily_arr_changes_org.sum_total_expansion_arr",
    "product_facts.crons_active_monitor_28d": "organizations_cron_monitoring.crons_active_monitor_28d",
    "product_facts.crons_checkins_daily": "organizations_cron_monitoring.crons_checkins_daily",
    "data_by_project.project_first_event_date_date": "sentry_projects.first_event_date",
    "data_by_project.errors_accepted_28d": "projects_data_outcomes.errors_accepted_28d",
    "data_by_project.transactions_accepted_28d": "projects_data_outcomes.transactions_accepted_28d",
    "data_by_project.proj_seer_issue_fixes_accepted": "projects_data_outcomes.seer_issue_fixes_accepted",
    "product_facts_integrations_array.individual_integration_features": "organizations_integrations.individual_integration_features",
    "data_by_project.proj_seer_issue_scans_accepted": "projects_data_outcomes.seer_issue_scans_accepted",
    "product_facts_autofix_llm.autofix_runs_sum": "organizations_autofix_usage.autofix_runs_sum",
    "data_by_sdkversion.errors": "sdk_base_events.errors",
    "data_by_project.proj_replays_accepted": "projects_data_outcomes.replays_accepted",
    "data_by_sdkversion.sdk_version_replays_support_proj": "sdk_mapping_minversion.replay_support",
    "sdk_mapping_minversion_project.transaction_support": "sdk_org_events.transaction_support",
    "data_by_sdkversion.sdk_version_crons_support_proj": "sdk_mapping_minversion.crons_support",
    "sdk_mapping_minversion_project.profile_support": "sdk_org_events.profile_support",
    "data_by_sdkversion.uses_latest_sdk_version": "sdk_base_events.uses_latest_sdk_version",
    "data_by_sdkversion.profiles": "sdk_base_events.profiles",
    "data_by_sdkversion.sdk_version": "sdk_base_events.sdk_version",
    "data_by_sdkversion.sdk_version_profiles_support_proj": "sdk_mapping_minversion.profile_support",
    "sdk_mapping_minversion_project.crons_support": "sdk_org_events.crons_support",
    "data_by_sdk.sdk_integrations_string": "sdk_base_events.sdk_integrations_string",
    "data_by_sdkversion.transactions": "sdk_base_events.transactions",
    "sdk_mapping_minversion_project.replay_support": "sdk_org_events.replay_support",
    "data_by_project.proj_spans_accepted": "projects_data_outcomes.daily_spans_accepted",
    "data_by_sdkversion.sdk_version_transactions_support_proj": "sdk_mapping_minversion.transaction_support",
    "data_by_project.profiles_accepted_28d": "projects_data_outcomes.profiles_accepted_28d",
    "data_by_project.attachments_accepted_28d": "projects_data_outcomes.attachments_accepted_28d",
    "data_by_project.spike_protection_disabled": "projects_configuration.spike_protection_disabled_flag",
    "data_by_project.regression_issue_alerts_count": "projects_alert_rules.count_of_alerts_for_regression",
    "data_by_project.sdk_integrations_enabled": "projects_events.sdk_integrations_flag",
    "data_by_project.new_issue_alerts_count": "projects_alert_rules.count_of_alerts_for_new_issues",
    "data_by_project.single_event_issue_percent_28d_proj": "projects_base_table.single_event_issue_percent_28d_proj",
    "data_by_project.messaging_integration_issue_alerts_count": "projects_alert_rules.count_of_messaging_integrations",
    "data_by_project.proj_team_member_count": "projects_team_assignments.team_members_count",
    "sdk_integrations_array.individual_sdk_integrations": "sdk_org_events.individual_sdk_integrations",
    "data_by_sdk.sessions_crash_free_sum": "sdk_base_events.sessions_crash_free_sum",
    "data_by_project.environment_based_metric_alerts_count": "projects_metric_alerts.environment_based_alert_rules_count",
    "data_by_project.client_side_sampling_used": "projects_data_outcomes.client_side_sampling_used",
    "data_by_project.server_side_filters_used": "projects_configuration.server_side_filters_used",
    "data_by_project.error_spike_metric_alerts_count": "projects_metric_alerts.error_spike_metric_alerts_count",
    "data_by_project.client_side_filters_used": "projects_data_outcomes.client_side_filters_used",
    "data_by_project.error_count_issue_alerts_count": "projects_alert_rules.count_of_alerts_for_issue_based_event_count",
    "data_by_project.transactional_data_metric_alerts_count": "projects_metric_alerts.transaction_based_alerts_count",
    "data_by_project.custom_tags_issue_alerts_count": "projects_alert_rules.count_of_alert_rules_with_custom_tags",
    "data_by_project.team_notification_issue_alert_count": "projects_alert_rules.count_of_alert_rules_notifying_team",
    "product_facts.sso_provider": "organizations_sso_configuration.provider",
    "product_facts.sso_users_28d": "organizations_sso_configuration.num_users_last_28days_from_snapshot",
    "product_facts.external_ticket_integration_flag": "organizations_integrations.external_ticket_integration_flag",
    "product_facts.sso_status": "organizations_sso_configuration.sso_status",
    "data_by_project.errors_over_quota_quarter_count": "projects_configuration.errors_over_quota_quarter_count",
    "data_by_project.transactions_over_quota_quarter_count": "projects_configuration.transactions_over_quota_quarter_count",
    "data_by_project.spend_allocation_enabled_flag": "projects_configuration.spend_allocation_enabled_flag",
    "data_by_project.replays_over_quota_quarter_count": "projects_configuration.replays_over_quota_quarter_count",
    "data_by_project.create_jira_ticket_issue_alert_count": "projects_alert_rules.count_of_alert_rules_to_create_ticket",
    "data_by_project.daily_resolved_issues_28d_proj": "projects_issues.daily_resolved_issues_28d_proj",
    "data_by_project.codemapping_enabled": "projects_configuration.codemapping_enabled_flag",
    "data_by_project.daily_new_issues_28d_proj": "projects_issues.daily_new_issues_28d_proj",
    "data_by_project.releases_created_through_cli": "projects_releases.releases_created_through_cli",
    "data_by_project.releases_having_commits_associated": "projects_releases.releases_having_commits_associated",
    "data_by_project.ownership_rules": "projects_configuration.number_of_ownership_rules",
    "data_by_project.daily_ignored_issues_28d_proj": "projects_issues.daily_ignored_issues_28d_proj",
    "product_facts.performance_units_accepted_28d": "projects_data_outcomes.performance_units_accepted_28d",
    "product_facts.transactions_utilization_rate_avg": "projects_data_outcomes.transactions_utilization_rate_avg",
    "product_facts.org_active_traces": "organizations_events.org_active_traces",
    "product_facts.spans_utilization_rate_avg": "projects_data_outcomes.spans_utilization_rate_avg",
    "accounts_billingmetricusage_on_org.sum_quantity_28d": "projects_data_outcomes.sum_quantity_28d",
    "product_facts.first_spans_date_week": "organizations_feature_adoption_dates.first_spans_week",
    "product_facts.first_spans_date_date": "organizations_feature_adoption_dates.first_spans_date_date",
    "data_by_sdk.spans_28d_proj_sdkfamily": "sdk_base_events.spans_28d_proj_sdkfamily",
    "data_by_sdk.transactions_28d_proj_sdkfamily": "sdk_base_events.transactions_28d_proj_sdkfamily",
    "product_facts.profiles_accepted_28d": "projects_data_outcomes.profiles_accepted_28d",
    "product_facts.sum_errors_rate_limited": "organizations_data_outcomes.errors_rate_limited",
    "expansion_churn_scores.slug": "organizations.slug",
    "product_facts.daily_new_performance_issues_28d_org": "projects_issues.daily_new_performance_issues_28d_proj",
    "product_facts.sum_indexed_transactions_rate_limited": "organizations_data_outcomes.indexed_transactions_rate_limited",
    "product_facts.indexed_transactions_accepted_28d": "projects_data_outcomes.indexed_transactions_accepted_28d",
    "product_facts.sum_indexed_transactions_filtered": "projects_data_outcomes.indexed_transactions_filtered",
    "product_facts.sum_errors_filtered": "projects_data_outcomes.errors_filtered",
    "data_by_sdk.number_of_errors_28d": "sdk_base_events.errors_28d_proj_sdk",
    "data_by_sdk.number_of_transactions_28d": "sdk_base_events.transactions_28d_proj_sdk",
    "data_by_project.indexed_transactions_accepted_28d": "projects_data_outcomes.indexed_transactions_accepted_28d",
    "data_by_sdk.number_of_events_28d": "sdk_base_events.events_28d_proj_sdk",
    "product_facts_alerts_array.primary_alertrule_id_slack": "projects_metric_alerts.primary_alertrule_id_slack",
    "product_facts_alerts_array.primary_alertrule_id_email": "projects_metric_alerts.has_email_action",
    "product_facts_alerts_array.primary_alertrule_id_pagerduty": "projects_metric_alerts.primary_alertrule_id_pagerduty",
    "product_facts_alerts_array.primary_alertrule_id": "projects_metric_alerts.primary_alertrule_id",
    "product_facts_alerts_array.primary_alertrule_id_msteams": "projects_metric_alerts.primary_alertrule_id_msteams",
    "product_facts_alerts_array.primary_alertrule_id_sentryapp": "projects_metric_alerts.has_sentryapp_action",
    "product_facts_alerts_array.primary_alertrule_id_active": "projects_metric_alerts.primary_alertrule_id_active",
    "product_facts.github_stacktrace_linked_successes": "organizations_analytics_specific.github_stacktrace_linked",
    "product_facts.created_dashboard_count": "organizations_analytics_specific.created_dashboard_count",
    "product_facts.transaction_summary_visit_count": "projects_analytics_specific.performance_transaction_summary_visits",
    "product_facts.performance_landing_page_visit_count": "organizations_analytics_specific.performance_landing_page_visits",
    "product_facts.opened_discover_query_count": "projects_analytics_specific.opened_discover_query",
    "product_facts.viewed_dashboard_count": "organizations_analytics_specific.viewed_dashboard_count",
    "data_by_project.proj_codeowner_rule_count": "projects_base_table.proj_codeowner_rule_count",
    "product_facts_analytics_array.percent_of_users_visiting_discover_query": "projects_analytics_summary.percent_of_users_visiting_discover_query",
    "product_facts.avg_org_active_users_28d": "organizations_age_tracking.avg_org_active_users_28d",
    "data_by_sdk.number_of_events": "sdk_base_events.events",
    "product_facts.sum_transactions_rate_limited": "projects_data_outcomes.transactions_rate_limited",
    "product_facts.sum_transactions_accepted": "projects_data_outcomes.transactions_accepted",
    "product_facts.total_metric_alerts_generated": "projects_analytics_specific.generated_metric_alerts",
    "product_facts.total_issue_alerts_generated": "projects_analytics_specific.generated_issue_alerts",
    "issues_by_type_struct.daily_new_performance_issues_sum": "issues_org_type.daily_new_performance_issues_sum",
    "issues_by_type_struct.daily_resolved_performance_issues_sum": "issues_base_type.daily_resolved_performance_issues",
    "product_facts.sum_replays_rate_limited": "projects_data_outcomes.sum_replays_rate_limited",
    "product_facts_features_array._individual_features": "organizations_integrations__all_integration_features_array._individual_features",
    "product_facts_events.web": "organizations_events.web",
    "product_facts_events.server": "organizations_events.server",
    "product_facts_events.desktop": "organizations_events.desktop",
    "product_facts_events.mobile": "organizations_events.mobile",
    "product_facts.sum_errors_invalid_abuse": "projects_data_outcomes.errors_invalid_abuse",
    "product_facts.sum_transactions_filtered": "organizations_data_outcomes.transactions_filtered",
    "product_facts.sum_transactions_invalid_abuse": "projects_data_outcomes.transactions_invalid_abuse",
    "product_facts.discover_activity_count": "organizations_analytics_specific.discover_activity_28d_org",
    "data_by_sdk.sessions_crashed_28d": "sdk_base_events.sessions_crashed_28d_proj_sdk",
    "data_by_sdk.sessions_abnormal_sum": "sdk_base_events.sessions_abnormal",
    "data_by_sdk.total_sessions_28d": "sdk_base_events.total_sessions_28d_proj_sdk",
    "data_by_sdk.crash_free_sessions_28d": "sdk_base_events.crash_free_sessions_28d",
    "data_by_sdk.sessions_crashed_sum": "sdk_base_events.sessions_crashed",
    "data_by_sdk.crash_free_rate_percent": "sdk_base_events.crash_free_rate_percent",
    "data_by_sdk.crash_free_rate": "sdk_base_events.crash_free_rate_percent",
    "data_by_sdk.total_sessions_sum": "sdk_base_events.total_sessions",
    "product_facts.org_id_link_to_engagement_score": "organizations_feature_flags.org_id_link_to_engagement_score",
    "product_facts.org_id_link_to_project_health": "organizations_feature_flags.org_id_link_to_project_health",
    "product_facts.sum_attachments_accepted": "organizations_data_outcomes.attachments_accepted",
    "product_facts.sum_profiles_accepted": "organizations_data_outcomes.profiles_accepted",
    "product_facts.org_active_mobile": "organizations_feature_flags.org_active_mobile",
    "product_facts.alert_rules": "organizations_feature_flags.alert_rules",
    "product_facts.source_maps": "organizations_feature_flags.source_maps",
    "product_facts.release_tracking": "organizations_feature_flags.release_tracking",
    "product_facts.custom_tags": "organizations_feature_flags.custom_tags",
    "product_facts.transactions_client_side_sampling_rate_28d": "organizations_analytics_summary.transactions_client_side_sampling_rate_28d",
    "product_facts.seer_issue_fixes_accepted_sum": "organizations_seer.seer_issue_fixes_accepted_sum",
    "product_facts.seer_issue_scans_accepted_sum": "organizations_seer.seer_issue_scans_accepted_sum",
    "user_details.given_name": "user_details_for_analytics.given_name",
    "user_details.username": "user_details_for_analytics.username",
    "user_details.full_name": "user_details_for_analytics.full_name",
    "emerge_first_adoption_dates.first_size_analysis_date": "organizations_feature_adoption_dates.first_size_analysis_date",
    "metric_type_events.median_size_bytes": "metric_type_events.median_size_bytes",
},
("forecasts_v2", "subscriptions_v3"): {},
("product_facts", "product_usage_sdk"): {
    "accounts_billingmetricusage_on_org.outcome_readable": "accounts_billingmetricusage_on_org.outcome_readable",
    "accounts_billingmetricusage_on_org.sum_quantity_28d": "projects_data_outcomes.sum_quantity_28d",
    "billing_model.billing_model": "daily_arr_by_category.billing_model",
    "billing_model_billing_category.sum_churn_arr": "daily_arr_changes_category.sum_total_churn_arr",
    "billing_model_billing_category.sum_contraction_arr": "daily_arr_changes_category.sum_total_contraction_arr",
    "billing_model_billing_category.sum_expansion_arr": "daily_arr_changes_category.sum_total_expansion_arr",
    "billing_model_billing_category.sum_new_arr": "daily_arr_changes_category.sum_total_new_arr",
    "billing_model_billing_category.sum_reactivation_arr": "daily_arr_changes_category.sum_total_reactivation_arr",
    "daily_financial_data_billing_category_struct.change_in_ondemand_arr": "daily_arr_changes_category.ondemand_change_arr",
    "data_by_project.attachments_accepted_28d": "projects_data_outcomes.attachments_accepted_28d",
    "data_by_project.client_side_filters_used": "projects_data_outcomes.client_side_filters_used",
    "data_by_project.client_side_sampling_used": "projects_data_outcomes.client_side_sampling_used",
    "data_by_project.codemapping_enabled": "projects_configuration.codemapping_enabled_flag",
    "data_by_project.create_jira_ticket_issue_alert_count": "projects_alert_rules.count_of_alert_rules_to_create_ticket",
    "data_by_project.custom_tags_issue_alerts_count": "projects_alert_rules.count_of_alert_rules_with_custom_tags",
    "data_by_project.daily_ignored_issues_28d_proj": "projects_issues.daily_ignored_issues_28d_proj",
    "data_by_project.daily_new_issues_28d_proj": "projects_issues.daily_new_issues_28d_proj",
    "data_by_project.daily_resolved_issues_28d_proj": "projects_issues.daily_resolved_issues_28d_proj",
    "data_by_project.environment_based_metric_alerts_count": "projects_metric_alerts.environment_based_alert_rules_count",
    "data_by_project.error_count_issue_alerts_count": "projects_alert_rules.count_of_alerts_for_issue_based_event_count",
    "data_by_project.error_spike_metric_alerts_count": "projects_metric_alerts.error_spike_metric_alerts_count",
    "data_by_project.errors_accepted_28d": "projects_data_outcomes.errors_accepted_28d",
    "data_by_project.errors_over_quota_quarter_count": "projects_configuration.errors_over_quota_quarter_count",
    "data_by_project.events_accepted_28d": "projects_data_outcomes.events_accepted_28d",
    "data_by_project.has_environments_current_enabled": "projects_configuration.has_environments_current_flag",
    "data_by_project.indexed_transactions_accepted_28d": "projects_data_outcomes.indexed_transactions_accepted_28d",
    "data_by_project.messaging_integration_issue_alerts_count": "projects_alert_rules.count_of_messaging_integrations",
    "data_by_project.new_issue_alerts_count": "projects_alert_rules.count_of_alerts_for_new_issues",
    "data_by_project.ownership_rules": "projects_configuration.number_of_ownership_rules",
    "data_by_project.primary_sdk": "projects_base_table.primary_sdk",
    "data_by_project.profiles_accepted_28d": "projects_data_outcomes.profiles_accepted_28d",
    "data_by_project.proj_codeowner_rule_count": "projects_base_table.proj_codeowner_rule_count",
    "data_by_project.proj_errors_accepted": "projects_data_outcomes.errors_accepted",
    "data_by_project.proj_replays_accepted": "projects_data_outcomes.replays_accepted",
    "data_by_project.proj_spans_accepted": "projects_data_outcomes.daily_spans_accepted",
    "data_by_project.proj_team_member_count": "projects_team_assignments.team_members_count",
    "data_by_project.proj_trace_metric_items_accepted": "projects_data_outcomes.trace_metric_items_accepted",
    "data_by_project.project_count": "projects_data_outcomes.project_count",
    "data_by_project.project_first_event_date_date": "sentry_projects.first_event_date",
    "data_by_project.project_id": "product_facts_v2_base.project_id",
    "data_by_project.project_name": "sentry_projects.name",
    "data_by_project.project_platform": "sentry_project.platform",
    "data_by_project.regression_issue_alerts_count": "projects_alert_rules.count_of_alerts_for_regression",
    "data_by_project.releases_created_through_cli": "projects_releases.releases_created_through_cli",
    "data_by_project.releases_having_commits_associated": "projects_releases.releases_having_commits_associated",
    "data_by_project.replays_accepted_28d": "projects_data_outcomes.replays_accepted_28d",
    "data_by_project.replays_over_quota_quarter_count": "projects_configuration.replays_over_quota_quarter_count",
    "data_by_project.sdk_integrations_enabled": "projects_events.sdk_integrations_flag",
    "data_by_project.server_side_filters_used": "projects_configuration.server_side_filters_used",
    "data_by_project.single_event_issue_percent_28d_proj": "projects_base_table.single_event_issue_percent_28d_proj",
    "data_by_project.spend_allocation_enabled_flag": "projects_configuration.spend_allocation_enabled_flag",
    "data_by_project.spike_protection_disabled": "projects_configuration.spike_protection_disabled_flag",
    "data_by_project.team_notification_issue_alert_count": "projects_alert_rules.count_of_alert_rules_notifying_team",
    "data_by_project.transactional_data_metric_alerts_count": "projects_metric_alerts.transaction_based_alerts_count",
    "data_by_project.transactions_accepted_28d": "projects_data_outcomes.transactions_accepted_28d",
    "data_by_project.transactions_over_quota_quarter_count": "projects_configuration.transactions_over_quota_quarter_count",
    "data_by_sdk.Total_replays_28d": "sdk_base_events.replays_28d_proj_sdk",
    "data_by_sdk.crash_free_rate": "sdk_base_events.crash_free_rate_percent",
    "data_by_sdk.crash_free_rate_percent": "sdk_base_events.crash_free_rate_percent",
    "data_by_sdk.crash_free_sessions_28d": "sdk_base_events.crash_free_sessions_28d",
    "data_by_sdk.events_28d_proj_sdkfamily": "sdk_base_events.events_28d_proj_sdkfamily",
    "data_by_sdk.log_size_bytes_28d": "sdk_base_events.log_size_bytes_28d_proj_sdk",
    "data_by_sdk.log_size_bytes_28d_proj_sdkfamily": "sdk_base_events.log_size_bytes_28d_proj_sdkfamily",
    "data_by_sdk.mobile_replays_28d_proj_sdkfamily": "sdk_base_events.mobile_replays_28d_proj_sdkfamily",
    "data_by_sdk.number_of_errors_28d": "sdk_base_events.errors_28d_proj_sdk",
    "data_by_sdk.number_of_events": "sdk_base_events.events",
    "data_by_sdk.number_of_events_28d": "sdk_base_events.events_28d_proj_sdk",
    "data_by_sdk.number_of_mobile_replays_28d": "sdk_base_events.mobile_replays_28d_proj_sdk",
    "data_by_sdk.number_of_transactions_28d": "sdk_base_events.transactions_28d_proj_sdk",
    "data_by_sdk.pduration_backend_28d_sdk_family": "sdk_base_events.pduration_backend_28d_proj_sdkfamily",
    "data_by_sdk.pduration_frontend_28d_sdk_family": "sdk_base_events.pduration_frontend_28d_proj_sdkfamily",
    "data_by_sdk.replays_28d_proj_sdkfamily": "sdk_base_events.replays_28d_proj_sdkfamily",
    "data_by_sdk.sdk_family": "sdk_map_new.language_family",
    "data_by_sdk.sdk_integrations_string": "sdk_base_events.sdk_integrations_string",
    "data_by_sdk.sdk_name": "sdk_base_events.sdk_name",
    "data_by_sdk.sessions_abnormal_sum": "sdk_base_events.sessions_abnormal",
    "data_by_sdk.sessions_crash_free_sum": "sdk_base_events.sessions_crash_free_sum",
    "data_by_sdk.sessions_crashed_28d": "sdk_base_events.sessions_crashed_28d_proj_sdk",
    "data_by_sdk.sessions_crashed_sum": "sdk_base_events.sessions_crashed",
    "data_by_sdk.spans_28d_proj_sdkfamily": "sdk_base_events.spans_28d_proj_sdkfamily",
    "data_by_sdk.total_accepted_replay_count": "sdk_base_events.replays_28d_proj_sdk",
    "data_by_sdk.total_pduration__28d_sdk_family": "sdk_base_events.total_pduration__28d_sdk_family",
    "data_by_sdk.total_sessions_28d": "sdk_base_events.total_sessions_28d_proj_sdk",
    "data_by_sdk.total_sessions_sum": "sdk_base_events.total_sessions",
    "data_by_sdk.trace_metric_items_28d": "sdk_base_events.trace_metric_items_28d_proj_sdk",
    "data_by_sdk.trace_metric_size_bytes_28d": "sdk_base_events.trace_metric_size_bytes_28d_proj_sdk",
    "data_by_sdk.transactions_28d_proj_sdkfamily": "sdk_base_events.transactions_28d_proj_sdkfamily",
    "data_by_sdkversion.errors": "sdk_base_events.errors",
    "data_by_sdkversion.log_size_bytes": "sdk_base_events.log_size_bytes",
    "data_by_sdkversion.logs_origin": "sdk_base_events.logs_origin",
    "data_by_sdkversion.median_log_size_bytes": "sdk_base_events.median_log_size_bytes",
    "data_by_sdkversion.median_metric_size_bytes": "sdk_base_events.median_trace_metric_size_bytes",
    "data_by_sdkversion.number_of_events": "sdk_base_events.events",
    "data_by_sdkversion.profile_duration_backend": "sdk_base_events.profile_duration_backend",
    "data_by_sdkversion.profile_duration_frontend": "sdk_base_events.profile_duration_frontend",
    "data_by_sdkversion.profiles": "sdk_base_events.profiles",
    "data_by_sdkversion.sdk_version": "sdk_base_events.sdk_version",
    "data_by_sdkversion.sdk_version_crons_support_proj": "sdk_mapping_minversion.crons_support",
    "data_by_sdkversion.sdk_version_profiles_support_proj": "sdk_mapping_minversion.profile_support",
    "data_by_sdkversion.sdk_version_replays_support_proj": "sdk_mapping_minversion.replay_support",
    "data_by_sdkversion.sdk_version_transactions_support_proj": "sdk_mapping_minversion.transaction_support",
    "data_by_sdkversion.total_profile_duration_continuous": "sdk_base_events.profile_duration_backend",
    "data_by_sdkversion.trace_metric_items": "sdk_base_events.trace_metric_items",
    "data_by_sdkversion.trace_metric_size_bytes": "sdk_base_events.trace_metric_size_bytes",
    "data_by_sdkversion.transactions": "sdk_base_events.transactions",
    "data_by_sdkversion.uses_latest_sdk_version": "sdk_base_events.uses_latest_sdk_version",
    "issues_by_type_struct.daily_new_performance_issues_sum": "issues_org_type.daily_new_performance_issues_sum",
    "issues_by_type_struct.daily_resolved_performance_issues_sum": "issues_base_type.daily_resolved_performance_issues",
    "metric_type_events.median_attributes": "projects_data_outcomes.median_attributes",
    "metric_type_events.metric_items": "projects_data_outcomes.metrics_accepted",
    "organization_uptime_summary.org_dt_total_active_monitors": "organizations.org_dt_total_active_monitors",
    "product_facts.active_organizations_count": "organizations.active_organizations_count",
    "product_facts.alert_rules": "organizations_feature_flags.alert_rules",
    "product_facts.avg_org_active_users_28d": "organizations_age_tracking.avg_org_active_users_28d",
    "product_facts.combined_integration_features": "organizations_integrations.all_integration_features_string",
    "product_facts.created_dashboard_count": "organizations_analytics_specific.created_dashboard_count",
    "product_facts.crons_active_monitor_1d": "organizations_cron_monitoring.crons_active_monitor_1d",
    "product_facts.custom_tags": "organizations_feature_flags.custom_tags",
    "product_facts.daily_new_performance_issues_28d_org": "projects_issues.daily_new_performance_issues_28d_proj",
    "product_facts.discover_activity_count": "organizations_analytics_specific.discover_activity_28d_org",
    "product_facts.dt_date": "product_facts_v2_base.dt_date",
    "product_facts.dt_fiscal_quarter": "product_facts_v2_base.dt_fiscal_quarter",
    "product_facts.dt_month": "product_facts_v2_base.dt_month",
    "product_facts.dt_week": "product_facts_v2_base.dt_week",
    "product_facts.errors_accepted_28d": "projects_data_outcomes.errors_accepted_28d",
    "product_facts.errors_utilization_rate": "projects_data_outcomes.errors_utilization_rate",
    "product_facts.external_ticket_integration_flag": "organizations_integrations.external_ticket_integration_flag",
    "product_facts.first_continuous_profile_date_date": "organizations_feature_adoption_dates.first_continuous_profile_date",
    "product_facts.first_logs_date_date": "organizations_feature_adoption_dates.first_logs_date_date",
    "product_facts.first_logs_date_month": "organizations_feature_adoption_dates.first_logs_date_month",
    "product_facts.first_spans_date_date": "organizations_feature_adoption_dates.first_spans_date_date",
    "product_facts.first_spans_date_week": "organizations_feature_adoption_dates.first_spans_week",
    "product_facts.first_ui_profile_date_date": "organizations_feature_adoption_dates.first_ui_profile_date_date",
    "product_facts.front_end_profile_duration_accepted_sum": "projects_data_outcomes.frontend_profile_duration_accepted",
    "product_facts.frontend_profile_duration_accepted_28d": "projects_data_outcomes.frontend_profile_duration_accepted_28d",
    "product_facts.github_integration": "organizations_integrations.github_integration",
    "product_facts.github_stacktrace_linked_successes": "organizations_analytics_specific.github_stacktrace_linked",
    "product_facts.indexed_spans_accepted_28d": "projects_data_outcomes.indexed_spans_accepted_28d",
    "product_facts.indexed_transactions_accepted_28d": "projects_data_outcomes.indexed_transactions_accepted_28d",
    "product_facts.is_last_day_of_month": "product_facts_v2_base.is_last_day_of_month",
    "product_facts.is_last_day_of_week": "product_facts_v2_base.is_last_day_of_week",
    "product_facts.logs_accepted_28d": "projects_data_outcomes.logs_bytes_accepted_28d",
    "product_facts.logs_count_28d": "projects_data_outcomes.logs_items_accepted_28d",
    "product_facts.opened_discover_query_count": "projects_analytics_specific.opened_discover_query",
    "product_facts.org_active": "organizations.is_active",
    "product_facts.org_active_backend": "organizations_feature_flags.org_active_backend",
    "product_facts.org_active_frontend": "sdk_map_new.framework_frontend",
    "product_facts.org_active_mobile": "organizations_feature_flags.org_active_mobile",
    "product_facts.org_active_traces": "organizations_events.org_active_traces",
    "product_facts.org_active_users_28d": "organizations_active_users.org_active_users_28d",
    "product_facts.org_age": "organizations_age_tracking.org_age",
    "product_facts.org_ea_flag": "organizations_analytics_summary.org_ea_flag",
    "product_facts.org_id_link_to_engagement_score": "organizations_feature_flags.org_id_link_to_engagement_score",
    "product_facts.org_id_link_to_project_health": "organizations_feature_flags.org_id_link_to_project_health",
    "product_facts.organization_id": "product_facts_v2_base.organization_id",
    "product_facts.organization_name": "organizations_analytics_summary.organization_name",
    "product_facts.organization_slug": "organizations.slug",
    "product_facts.organizations_count": "product_facts_v2_base.count",
    "product_facts.performance_landing_page_visit_count": "organizations_analytics_specific.performance_landing_page_visits",
    "product_facts.performance_units_accepted": "projects_data_outcomes.performance_units_accepted",
    "product_facts.performance_units_accepted_28d": "projects_data_outcomes.performance_units_accepted_28d",
    "product_facts.profile_duration_accepted_28d": "organizations_data_outcomes.profile_duration_accepted_28d",
    "product_facts.profile_duration_accepted_sum": "projects_data_outcomes.profile_duration_accepted",
    "product_facts.profiles_accepted_28d": "projects_data_outcomes.profiles_accepted_28d",
    "product_facts.release_tracking": "organizations_feature_flags.release_tracking",
    "product_facts.replays_accepted_28d": "projects_data_outcomes.replays_accepted_28d",
    "product_facts.seer_issue_fixes_accepted_sum": "organizations_seer.seer_issue_fixes_accepted_sum",
    "product_facts.seer_issue_scans_accepted_sum": "organizations_seer.seer_issue_scans_accepted_sum",
    "product_facts.source_maps": "organizations_feature_flags.source_maps",
    "product_facts.spans_accepted_28d": "organizations_data_outcomes.spans_accepted_28d",
    "product_facts.spans_utilization_rate_avg": "projects_data_outcomes.spans_utilization_rate_avg",
    "product_facts.sso_provider": "organizations_sso_configuration.provider",
    "product_facts.sso_status": "organizations_sso_configuration.sso_status",
    "product_facts.sso_users_28d": "organizations_sso_configuration.num_users_last_28days_from_snapshot",
    "product_facts.sum_active_projects": "organizations_data_outcomes.org_active_projects",
    "product_facts.sum_attachments_accepted": "organizations_data_outcomes.attachments_accepted",
    "product_facts.sum_errors_accepted": "projects_data_outcomes.errors_accepted",
    "product_facts.sum_errors_filtered": "projects_data_outcomes.errors_filtered",
    "product_facts.sum_errors_invalid_abuse": "projects_data_outcomes.errors_invalid_abuse",
    "product_facts.sum_errors_rate_limited": "organizations_data_outcomes.errors_rate_limited",
    "product_facts.sum_indexed_transactions_filtered": "projects_data_outcomes.indexed_transactions_filtered",
    "product_facts.sum_indexed_transactions_rate_limited": "organizations_data_outcomes.indexed_transactions_rate_limited",
    "product_facts.sum_logs_accepted": "projects_data_outcomes.logs_bytes_accepted",
    "product_facts.sum_org_active_users_28d": "organizations.active_users_28d",
    "product_facts.sum_org_users_total": "organizations_active_users.org_users_total",
    "product_facts.sum_profiles_accepted": "organizations_data_outcomes.profiles_accepted",
    "product_facts.sum_replays_accepted": "projects_data_outcomes.replays_accepted",
    "product_facts.sum_replays_rate_limited": "projects_data_outcomes.sum_replays_rate_limited",
    "product_facts.sum_spans_accepted": "projects_data_outcomes.daily_spans_accepted",
    "product_facts.sum_transactions_accepted": "projects_data_outcomes.transactions_accepted",
    "product_facts.sum_transactions_filtered": "organizations_data_outcomes.transactions_filtered",
    "product_facts.sum_transactions_invalid_abuse": "projects_data_outcomes.transactions_invalid_abuse",
    "product_facts.sum_transactions_rate_limited": "projects_data_outcomes.transactions_rate_limited",
    "product_facts.total_issue_alerts_generated": "projects_analytics_specific.generated_issue_alerts",
    "product_facts.total_metric_alerts_generated": "projects_analytics_specific.generated_metric_alerts",
    "product_facts.total_profile_duration_accepted_28d": "projects_data_outcomes.profile_duration_accepted_28d",
    "product_facts.total_profile_duration_accepted_sum": "organizations_data_outcomes.profile_duration_accepted",
    "product_facts.trace_metric_items_accepted_28d": "organizations_data_outcomes.trace_metric_items_accepted_28d",
    "product_facts.trace_metric_items_accepted_sum": "projects_data_outcomes.trace_metric_items_accepted",
    "product_facts.transaction_summary_visit_count": "projects_analytics_specific.performance_transaction_summary_visits",
    "product_facts.transactions_accepted_28d": "projects_data_outcomes.transactions_accepted_28d",
    "product_facts.transactions_client_side_sampling_rate_28d": "organizations_analytics_summary.transactions_client_side_sampling_rate_28d",
    "product_facts.transactions_utilization_rate": "projects_data_outcomes.transactions_utilization_rate",
    "product_facts.transactions_utilization_rate_avg": "projects_data_outcomes.transactions_utilization_rate_avg",
    "product_facts.viewed_dashboard_count": "organizations_analytics_specific.viewed_dashboard_count",
    "product_facts_alerts_array.primary_alertrule_id": "projects_metric_alerts.primary_alertrule_id",
    "product_facts_alerts_array.primary_alertrule_id_active": "projects_metric_alerts.primary_alertrule_id_active",
    "product_facts_alerts_array.primary_alertrule_id_email": "projects_metric_alerts.has_email_action",
    "product_facts_alerts_array.primary_alertrule_id_msteams": "projects_metric_alerts.primary_alertrule_id_msteams",
    "product_facts_alerts_array.primary_alertrule_id_pagerduty": "projects_metric_alerts.primary_alertrule_id_pagerduty",
    "product_facts_alerts_array.primary_alertrule_id_sentryapp": "projects_metric_alerts.has_sentryapp_action",
    "product_facts_alerts_array.primary_alertrule_id_slack": "projects_metric_alerts.primary_alertrule_id_slack",
    "product_facts_analytics_array.percent_of_users_visiting_discover_query": "projects_analytics_summary.percent_of_users_visiting_discover_query",
    "product_facts_events.desktop": "organizations_events.desktop",
    "product_facts_events.event_type": "organizations_events.event_type",
    "product_facts_events.events_28d": "organizations_data_outcomes.events_accepted_28d",
    "product_facts_events.mobile": "organizations_events.mobile",
    "product_facts_events.number_of_events_sum": "organizations_events.number_of_events_sum",
    "product_facts_events.sdk_family": "organizations_events.sdk_family",
    "product_facts_events.sdk_name": "organizations_events.sdk_name",
    "product_facts_events.server": "organizations_events.server",
    "product_facts_events.web": "organizations_events.web",
    "product_facts_features_array._individual_features": "organizations_integrations__all_integration_features_array._individual_features",
    "product_facts_org_rca_feedback.changes_negative_feedback": "product_facts_org_rca_feedback.changes_negative_feedback",
    "product_facts_org_rca_feedback.rca_negative_feedback": "product_facts_org_rca_feedback.rca_negative_feedback",
    "product_facts_org_rca_feedback.rca_positive_feedback": "product_facts_org_rca_feedback.rca_positive_feedback",
    "product_facts_org_rca_feedback.solution_positive_feedback": "product_facts_org_rca_feedback.rca_positive_feedback",
    "sdk_base_events.replays_28d_proj_sdk": "sdk_base_events.replays_28d_proj_sdk",
    "sdk_integrations_array.individual_sdk_integrations": "sdk_org_events.individual_sdk_integrations",
    "sdk_mapping_minversion_project.crons_support": "sdk_org_events.crons_support",
    "sdk_mapping_minversion_project.profile_support": "sdk_org_events.profile_support",
    "sdk_mapping_minversion_project.replay_support": "sdk_org_events.replay_support",
    "sdk_mapping_minversion_project.transaction_support": "sdk_org_events.transaction_support",
    "subscriptions_v3.total_new_arr": "daily_arr_changes_org.sum_total_new_arr",
    "trial_view.trial_start_month": "organizations_trial_view.trial_start_month",
    "user_details.full_name": "user_details.full_name",
    "user_details.username": "user_details_for_analytics.username",
    "user_details_for_analytics.email": "user_details_for_analytics.email",
    "user_details_for_analytics.full_name": "user_details_for_analytics.full_name",
    "user_details_for_analytics.is_staff": "user_details_for_analytics.is_staff",
    "user_details_for_analytics.is_superuser": "user_details_for_analytics.is_superuser",
},
}

FIELD_MAP = FIELD_MAPS.get((OLD_EXPLORE, NEW_EXPLORE), {})


def get_field_map():
    return FIELD_MAPS.get((OLD_EXPLORE, NEW_EXPLORE), {})


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Looker dashboard migration tool")
    p.add_argument("--source",        required=False, default=None, help="Source dashboard ID (copy FROM)")
    p.add_argument("--batch",         nargs="+", metavar="ID", help="Validate multiple source dashboard IDs (or SOURCE:DEST pairs)")
    p.add_argument("--dest",          required=False, default=None, help="Destination dashboard ID (copy TO)")
    p.add_argument("--dry-run",       action="store_true", help="Preview changes without writing")
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
            return NEW_EXPLORE_2
        if view in exclusive1:
            return NEW_EXPLORE
    print(f"  ⚠️  route_explore: no exclusive view found in fields {fields} — defaulting to {NEW_EXPLORE}")
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
# CHECK TILES
# ─────────────────────────────────────────────
def _collect_tile_fields(q):
    """Return all LookML field references in a query as {field: source_label}."""
    fields = {}
    for f in (q.fields or []):
        if "." in f and not f.startswith("__"):
            fields[f] = "field"
    for f in (q.filters or {}).keys():
        if "." in f and not f.startswith("__"):
            fields[f] = "filter"
    for s in (q.sorts or []):
        f = s.split(" ")[0]
        if "." in f and not f.startswith("__"):
            fields[f] = "sort"
    if q.dynamic_fields:
        try:
            for d in json.loads(q.dynamic_fields):
                label = d.get("label") or d.get("table_calculation") or "(unnamed)"
                f = d.get("based_on", "")
                if f and "." in f and not f.startswith("__"):
                    fields[f] = f"dynamic '{label}' based_on"
                for ref in re.findall(r'\$\{([^}]+)\}', d.get("expression") or ""):
                    if "." in ref and not ref.startswith("__"):
                        fields[ref] = f"dynamic '{label}' expression"
                for fk in (d.get("filters") or {}).keys():
                    if "." in fk and not fk.startswith("__"):
                        fields[fk] = f"dynamic '{label}' filter"
        except Exception:
            pass
    return fields


def check_tiles(sdk, source_id):
    print(f"\n=== Checking source dashboard {source_id} against {NEW_MODEL}/{NEW_EXPLORE} ===\n")

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

    summary_bad           = {}   # old_field -> dest_field
    summary_missing_join  = set()
    summary_missing_field = set()

    for el in elements:
        if not el.query_id:
            continue
        q = sdk.query(str(el.query_id))
        tile_title = el.title or "(untitled)"

        fields = _collect_tile_fields(q)
        if not fields:
            continue

        ok, mapped, bad, missing_join, missing_field = [], [], [], [], []

        for f in sorted(fields):
            if f in FIELD_MAP:
                dest = FIELD_MAP[f]
                if dest in dest_fields:
                    mapped.append((f, dest))
                else:
                    bad.append((f, dest))
                    summary_bad[f] = dest
            elif f in dest_fields:
                ok.append(f)
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
            print(f"    🔴 {f}  (view not joined into explore)  [{fields[f]}]")
        for f in missing_field:
            print(f"    🟡 {f}  (view is joined but field doesn't exist in LookML)  [{fields[f]}]")
        print()

    any_issues = summary_bad or summary_missing_join or summary_missing_field
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

    return not any_issues


def validate(sdk, source_id):
    """Deprecated — use check()."""
    print("(--validate is deprecated; running --check instead)")
    return check(sdk, source_id)

def check_explore(sdk, source_id):
    """Deprecated — use check()."""
    print("(--check-explore is deprecated; running --check instead)")
    return check(sdk, source_id)

def audit(sdk, source_id):
    """Deprecated — use check()."""
    print("(--audit is deprecated; running --check instead)")
    return check(sdk, source_id)


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
# STEP 1b: Copy vis_config from source
# ─────────────────────────────────────────────
def copy_vis_config_from_source(sdk, source_id, dest_id, dry_run):
    print(f"\n=== Step 1b: Copy vis_config from source dashboard {source_id} ===")
    # Cache explore fields once for WILL BREAK checks (both explores)
    try:
        _explore_fields = set()
        for _explore_name in (NEW_EXPLORE, NEW_EXPLORE_2):
            _exp = sdk.lookml_model_explore(NEW_MODEL, _explore_name, fields="fields")
            for _f in (_exp.fields.dimensions or []):
                _explore_fields.add(_f.name)
            for _f in (_exp.fields.measures or []):
                _explore_fields.add(_f.name)
    except Exception:
        _explore_fields = set()

    source_elements = sdk.dashboard_dashboard_elements(source_id, fields="id,title,query_id,result_maker,row,col")
    dest_elements   = sdk.dashboard_dashboard_elements(dest_id, fields="id,title,query_id,result_maker,row,col")

    source_by_title    = {}
    source_by_position = {}
    for el in source_elements:
        vc, loc = extract_vis_config(el)
        if not vc:
            continue
        title_key = (el.title or "").strip().lower()
        if title_key:
            source_by_title[title_key] = (vc, loc, el)
        else:
            source_by_position[(el.row, el.col)] = (vc, loc, el)

    print(f"  Found vis_config for {len(source_by_title) + len(source_by_position)} tiles in source")

    for el in dest_elements:
        if not el.query_id:
            continue
        title_key = (el.title or "").strip().lower()

        # Match by title first, then fall back to position for untitled tiles
        if title_key and title_key in source_by_title:
            vc, loc, src_el = source_by_title[title_key]
        elif not title_key and (el.row, el.col) in source_by_position:
            vc, loc, src_el = source_by_position[(el.row, el.col)]
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
        vc, _ = extract_vis_config(el, q)
        remapped_fields  = remap_fields(q.fields, el.title)
        remapped_filters = remap_filters(q.filters, el.title)
        remapped_sorts   = remap_sorts(q.sorts)
        target_explore = route_explore(
            list(remapped_fields or []) + list((remapped_filters or {}).keys()),
            _EXCLUSIVE_1, _EXCLUSIVE_2,
        )
        if dry_run:
            for s in (q.sorts or []):
                if is_problem_field(s.split(" ")[0]):
                    print(f"  ⚠️  WILL BREAK '{el.title}' — sort not available in new explore: {s}")
            print(f"  [DRY RUN] Would swap '{el.title}' → {target_explore}")
            continue
        new_query = sdk.create_query(
            models.WriteQuery(
                model=NEW_MODEL,
                view=target_explore,
                fields=remapped_fields,
                filters=remapped_filters,
                sorts=remapped_sorts,
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
    FIELD_MAP   = get_field_map()

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

    # --check-tiles runs in dev so it sees fields on the migration branch
    if args.check_tiles:
        ok = check_tiles(sdk, args.source)
        sys.exit(0 if ok else 1)

    # Load both explore view sets for routing and is_problem_field
    try:
        _views1, _views2, _excl1, _excl2 = build_explore_view_sets(sdk)
        _EXCLUSIVE_1.update(_excl1)
        _EXCLUSIVE_2.update(_excl2)
        _EXPLORE_VIEWS.update(_views1 | _views2)
    except Exception as _e:
        print(f"⚠️  Could not load explore fields: {_e}")

    dry_run   = args.dry_run
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

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Migrating dashboard {source_id} → {dest_id}")

    # full migration (dry-run or live)
    snapshot(sdk, dest_id, dry_run)
    fix_dashboard_filters(sdk, dest_id, dry_run)
    swap_and_fix_tiles(sdk, dest_id, dry_run)
    copy_vis_config_from_source(sdk, source_id, dest_id, dry_run)
    reconnect_dashboard_filters(sdk, source_id, dest_id, dry_run)
    verify(sdk, dest_id)
    print(f"\n✓ Done — snapshot saved to snapshot_{dest_id}.json")
