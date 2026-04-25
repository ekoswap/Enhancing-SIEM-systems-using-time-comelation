from pipeline.dns_strategy import DNS_COLUMNS, dns_best_aggregation
from pipeline.flows_strategy import FLOWS_COLUMNS, flows_filter_duration_only, flows_best_aggregation
from pipeline.proc_strategy import PROC_COLUMNS, proc_filter_end_only, proc_best_aggregation
from pipeline.auth_strategy import AUTH_COLUMNS, auth_filter_improved, auth_exact_dedup, auth_best_aggregation


SOURCE_REGISTRY = {
    "dns": {
        "columns": DNS_COLUMNS,
        "filter_fn": None,
        "dedup_fn": None,
        "aggregate_fn": dns_best_aggregation,
        "src_col": "source_computer",
        "dst_col": "destination_computer",
        "match_mode": "pair",
        "requires_preparation": False,
        "preparation_fn": None,
    },
    "flows": {
        "columns": FLOWS_COLUMNS,
        "filter_fn": flows_filter_duration_only,
        "dedup_fn": None,
        "aggregate_fn": flows_best_aggregation,
        "src_col": "source_computer",
        "dst_col": "destination_computer",
        "match_mode": "pair",
        "requires_preparation": False,
        "preparation_fn": None,
    },
    "proc": {
        "columns": PROC_COLUMNS,
        "filter_fn": proc_filter_end_only,
        "dedup_fn": None,
        "aggregate_fn": proc_best_aggregation,
        "src_col": "computer",
        "dst_col": "computer",
        "match_mode": "multi",
        "match_columns": ["user", "computer"],
        "requires_preparation": False,
        "preparation_fn": None,
    },
    "auth": {
    "columns": AUTH_COLUMNS,
    "filter_fn": auth_filter_improved,
    "dedup_fn": auth_exact_dedup,
    "aggregate_fn": auth_best_aggregation,
    "src_col": "src_computer",
    "dst_col": "dst_computer",
    "match_mode": "multi",
    "match_columns": ["src_user", "src_computer", "dst_computer"],
    "requires_preparation": True,
    "preparation_fn": "prepare_auth_dataframe",
},
}