"""Expectation suite definitions per domain.

Each domain has a set of expectations that define data quality rules.
These are used by the validation runner to check pipeline outputs
before they're deployed to production.
"""

type ExpectationConfig = dict[str, str | dict]
type SuiteConfig = list[ExpectationConfig]
type DomainName = str


# Per-domain expectation suites — these define the quality contract
# for each pipeline output.
_DOMAIN_SUITES: dict[DomainName, SuiteConfig] = {
    "sales": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "transaction_id"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "transaction_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "transaction_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "amount", "min_value": 0, "max_value": 1_000_000},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "customer_id"},
        },
    ],
    "inventory": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "sku"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "sku"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "quantity", "min_value": 0},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "warehouse_id",
                "value_set": ["DC-001", "DC-002", "FC-010", "CS-003", "BK-050"],
            },
        },
    ],
    "finance": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "account_code"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "account_code"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "fiscal_period"},
        },
        {
            "expectation_type": "expect_column_pair_values_to_be_equal",
            "kwargs": {
                "column_A": "debit",
                "column_B": "credit",
                "or_equal": True,
            },
        },
    ],
    "hr": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "employee_id"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "employee_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "employee_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "status",
                "value_set": ["active", "inactive", "terminated", "on_leave"],
            },
        },
    ],
    "logistics": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "shipment_id"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "shipment_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "weight_kg", "min_value": 0, "max_value": 50_000},
        },
    ],
    "marketing": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "campaign_id"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "campaign_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "spend", "min_value": 0},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "impressions", "min_value": 0},
        },
    ],
    "procurement": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "po_number"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "po_number"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "total_amount", "min_value": 0},
        },
    ],
    "support": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "ticket_id"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "ticket_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "priority",
                "value_set": ["low", "medium", "high", "critical"],
            },
        },
    ],
    "quality": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "inspection_id"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "inspection_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "result",
                "value_set": ["pass", "fail", "conditional"],
            },
        },
    ],
    "manufacturing": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "work_order_id"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "work_order_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "yield_pct", "min_value": 0, "max_value": 100},
        },
    ],
}


def build_suite_for_domain(domain: DomainName) -> SuiteConfig:
    """Return the expectation suite for a domain, or a sensible default."""
    if domain in _DOMAIN_SUITES:
        return _DOMAIN_SUITES[domain]

    # Default suite: just check that the dataframe is not empty
    return [
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1},
        },
    ]
