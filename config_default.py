"""
This module provides default parameters for the configuration.

"""

from pyspark.sql.types import TimestampType, StringType, DoubleType


class DataGenerationConfig:
    """
        A class providing configuration parameters for the data generation run.
    """

    latest_year_dash_month = None  # "2022-11"
    time_extent_in_month = 12

    data_size = "tiny"

    # base data
    base_size = 100
    scale_factor = 1
    # deviation data
    num_typologies = 1
    typology_cust_size_wrt_normal = 0.1
    suspicious_cases_per_typo = 2

    individual_to_corporate_count_ratio = 4
    corporate_to_individual_revenue_ratio = 100
    turnover_to_revenue_ratio = 2
    local_to_foreigner_count_ratio = 4

    fill_all_columns = True
    hide_empty_columns = False
    drop_empty_columns = False

    data_version = "6f0ec42c58"
    use_case = "dev_or_demo"
    product_version = "5.5.0"
    respect_existing_db_schema = True

    input_typologies = []
    product = "pam"
    sam_random_properties_flag = False
    unstable = True

    max_records_per_partition = 100
    estimated_memory_per_customer_in_mb = 10
    hive_node_ip = "localhost"
    db_name = ""
    hbase_ip = ""
    schema_name = ""
    write_all_hive_tables_even_empty_ones = False
    tables_metadata = {'CUSTOMER': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'CUSTOMER_TO_CUSTOMER': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'HIGH_RISK_PARTY': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'TRANSACTIONS': {'external_table': True, 'partitioned_by': ['TXN_DATE_TIME_YEAR_MONTH']}, 'CDD_ALERTS': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'CDD_THRESHOLD': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR']}, 'CDD_ALERTS_HISTORY': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'CDD_MISSING_ALERT_HISTORY': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'CDD_SAMPLING_PARAMS': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR']}, 'ACCOUNTS_PRECOMPUTE_TABLE': {'external_table': False, 'partitioned_by': ''}, 'CUSTOMERS_PRECOMPUTE_TABLE': {'external_table': False, 'partitioned_by': ''}, 'SUBGRAPH_FEATURE': {'external_table': False, 'partitioned_by': ['CREATION_DATETIME']}, 'PRECOMPUTE_CUSTOMER_TXN_BEHAVIOR_MONTHLY': {'external_table': False, 'partitioned_by': ['YEAR_MONTH_NUM']}, 'PRECOMPUTE_CUSTOMER_TXN_BEHAVIOR_WEEKLY': {'external_table': False, 'partitioned_by': ['YEAR_WEEK_NUM']}, 'PRECOMPUTE_TWO_ENTITY_BEHAVIOR_WEEKLY': {'external_table': False, 'partitioned_by': ['YEAR_WEEK_NUM']}, 'PRECOMPUTE_TWO_ENTITY_BEHAVIOR_MONTHLY': {'external_table': False, 'partitioned_by': ['YEAR_MONTH_NUM']}, 'IAD_MODEL_PARAMETERS': {'external_table': False, 'partitioned_by': ''}, 'PRECOMPUTE_GRAPH_MOTIF': {'external_table': False, 'partitioned_by': ''}, 'NS_ALERTS': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'WATCHLIST': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA']}, 'NS_DICTIONARY': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA']}, 'NS_ALERTS_HISTORY': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'NS_HITS_HISTORY': {'external_table': False, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'NS_MISSING_ALERT_HISTORY': {'external_table': False, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'CUSTOM_LIST': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA']}, 'CUSTOMER_TO_ACCOUNT': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'ACCOUNTS': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'CUSTOMER_ADDRESSES': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'CUSTOMER_CONTACT': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'FIXED_DEPOSITS': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'LOANS': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'CODES': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA']}, 'HIGH_RISK_COUNTRY': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA']}, 'FIG_MAPPING': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA']}, 'TM_ALERTS': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'CARDS': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'TM_THRESHOLD': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR']}, 'TM_ALERTS_HISTORY': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'ALERTED_CUSTOMERS_HISTORY': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'ALERTED_CUSTOMERS_LINKED_CUSTOMERS_HISTORY': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'ALERTED_CUSTOMERS_TO_ACCOUNTS_HISTORY': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'CUSTOMER_CLUSTER_RISK_INDICATOR_STATS_HISTORY': {'external_table': False, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'MODEL_OUTPUT_HISTORY': {'external_table': False, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'FEATURE_GROUP_HISTORY': {'external_table': False, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'FEATURE_TYPOLOGY_HISTORY': {'external_table': False, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'TM_MISSING_ALERT_HISTORY': {'external_table': False, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'TM_SAMPLING_PARAMS': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR']}, 'FEATURE_GROUP': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'FEATURE_TYPOLOGY': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR_MONTH']}, 'UNSUPERVISED_MODEL_PARAMETERS': {'external_table': True, 'partitioned_by': ''}, 'TS_ALERTS': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA', 'TT_UPDATED_YEAR_MONTH']}, 'TS_CODES': {'external_table': True, 'partitioned_by': ['TT_IS_LATEST_DATA']}, 'TS_SAMPLING_PARAMS': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR']}, 'TS_THRESHOLD': {'external_table': True, 'partitioned_by': ['TT_UPDATED_YEAR']}}

    db_write_mode = "overwrite"
    write_to_file = False
    return_data_frames = True
    result_df_ordering = ["CUSTOMER", "CUSTOMER_TO_CUSTOMER", "ACCOUNTS", "CUSTOMER_TO_ACCOUNT", "TRANSACTIONS",
                          "CARDS", "FIXED_DEPOSITS", "LOANS",
                          "HIGH_RISK_COUNTRY", "HIGH_RISK_PARTY",
                          "CDD_ALERTS", "NS_ALERTS", "TM_ALERTS", "UNIFIED_ALERTS",
                          "WATCHLIST"]

    name_of_watchlist = "watchlist_cases"
    tt_updated_from_date = "01-01-2019"
    tt_updated_to_date = "31-12-2022"
    number_watchlist_records = 1000
    number_customer_records = 1000
    transaction_records = 500

    use_input_data = False

    partial_custom_typology = False
    partial_custom_typology_rename_map = {
        "created_at": ["TRANSACTIONS", "TXN_DATE_TIME", TimestampType()],
        "tx_type_desc": ["TRANSACTIONS", "TXN_TYPE_CATEGORY", StringType()],
        "currency": ["TRANSACTIONS", "ACCT_CURRENCY_CODE", StringType()],
        "amount": ["TRANSACTIONS", "TXN_AMOUNT", DoubleType()],
        "balance": ["ACCOUNTS", "OPENING_BALANCE", DoubleType()],
        "deposit_from_address": ["TRANSACTIONS", "OPP_ACCOUNT_KEY", StringType()],
        "withdraw_to_address": ["TRANSACTIONS", "OPP_ACCOUNT_KEY", StringType()],
        "user_id": ["TRANSACTIONS", "PRIMARY_PARTY_KEY", StringType()]
    }

    txn_category_custom_mapping = {
        "Deposit": "incoming-e-wallet",
        "Withdraw": "outgoing-e-wallet",
        "Refund": "incoming-e-wallet"
    }

    column_equivalence_sets = [
        [["CUSTOMER", "PARTY_KEY"],
         ["CUSTOMER_TO_CUSTOMER", "PARTY_KEY"],
         ["ACCOUNTS", "PRIMARY_PARTY_KEY"],
         ["CUSTOMER_TO_ACCOUNT", "PARTY_KEY"],
         ["TRANSACTIONS", "PRIMARY_PARTY_KEY"]],
        [["ACCOUNTS", "CURRENCY_CODE"],
         ["TRANSACTIONS", "ACCT_CURRENCY_CODE"]]
    ]

    seed = 0
    random_seed_is_random = True


class DataModelConfig:
    """
        A class providing configuration parameters for the data generation.
    """

    txn_categories_incoming = ["CDM-cash-deposit", "cash-equivalent-deposit", "cash-equivalent-card-payment",
                               "incoming-local-fund-transfer", "incoming-overseas-fund-transfer",
                               "incoming-card-payment", "incoming-card",
                               "incoming-loan-payment",
                               "incoming-e-wallet",
                               "Misc-credit", "voucher-sale"]
    txn_categories_outgoing = ["ATM-withdrawal", "cash-equivalent-withdrawal", "card-charge",
                               "outgoing-local-fund-transfer", "outgoing-overseas-fund-transfer",
                               "outgoing-card-payment", "outgoing-card",
                               "outgoing-loan-payment",
                               "outgoing-e-wallet",
                               "Misc-debit", "voucher-purchase"]
    txn_categories = txn_categories_incoming + txn_categories_outgoing

    # all valid account types: ["FIXED DEPOSITS", "LOANS", "CREDIT CARDS", "SAVINGS"]
    acct_type_to_valid_txn_categories = {
        "SAVINGS": {
            "incoming": [x for x in txn_categories_incoming if x not in ["incoming-loan-payment", "voucher-sale"]],
            "outgoing": [x for x in txn_categories_outgoing if x not in ["outgoing-loan-payment", "voucher-purchase"]],
        },
        "CREDIT CARDS": {
            "incoming": ["incoming-card-payment", "incoming-card",  "cash-equivalent-deposit", "Misc-credit"],
            "outgoing": ["outgoing-card-payment", "outgoing-card", "Misc-debit"],
        },
        "FIXED_DEPOSITS": {
            "incoming": ["incoming-local-fund-transfer", "cash-equivalent-deposit", "Misc-credit"],
            "outgoing": ["outgoing-local-fund-transfer", "cash-equivalent-withdrawal", "Misc-debit"],
        },
        "LOANS": {
             "incoming": ["incoming-loan-payment", "incoming-local-fund-transfer", "cash-equivalent-deposit",
                          "Misc-credit"],
             "outgoing": ["outgoing-loan-payment", "outgoing-local-fund-transfer", "cash-equivalent-withdrawal",
                          "Misc-debit"],
        },
    }

    txn_categories_atm = ["CDM-cash-deposit", "ATM-withdrawal"]

    txn_categories_cash = ["CDM-cash-deposit", "cash-equivalent-deposit", "ATM-withdrawal",
                           "cash-equivalent-withdrawal"]

    txn_categories_voucher = ["voucher-purchase", "voucher-sale"]

    txn_categories_fund_transfer_ewallet = ["incoming-local-fund-transfer", "incoming-overseas-fund-transfer",
                                            "incoming-e-wallet", "outgoing-local-fund-transfer",
                                            "outgoing-overseas-fund-transfer", "outgoing-e-wallet"]

    txn_categories_overseas = ["incoming-overseas-fund-transfer", "outgoing-overseas-fund-transfer"]

    txn_categories_loans = ["incoming-loan-payment", "outgoing-loan-payment"]


class BaseConfig:
    """
        A class providing configuration parameters for the base data.
    """
    prefix = "base"

    min_account_number = 1
    max_account_number = 3

    transaction_amount = list(range(40, 201))
    min_txn_account_per_customer_per_month = 90

    transaction_count = list(range(int(round(min_txn_account_per_customer_per_month / max_account_number)),
                                   int(round(
                                            (3 * min_txn_account_per_customer_per_month) / max_account_number))))

    party_annual_income = list(range(45000, 650000))  # about the range of frequency * #amount * 12
    monthly_avg_balance = [income * 1/12 for income in party_annual_income]

    loan_to_income_factor = 20

    proportion_linked = 1.0
    min_linked_party_num = 1
    max_linked_party_num = 4

    known_to_unknown_factor = 5
    opposite_party_set_size = list(range(min_linked_party_num, max(min_linked_party_num + 1,
                                                                   max_linked_party_num * known_to_unknown_factor)
                                         )
                                   )


class PropertyConfig:
    """
        A class providing configuration parameters for the deviation data.
    """
    # this is used to scale most of the values below
    demo_factor = 1

    # for training a model use subtle values
    surge_count_factor = 1.4
    surge_amount_factor = 1.4
    high_transaction_amount_factor = 1.4
    high_transaction_count_factor = 1.4

    account_set_size_factor = 2
    party_set_size_factor = 20
    look_back = 1  # months

    structuring_lower_bound = 50000
    structuring_upper_bound = 100000

    expense_compared_to_revenue_or_income = 12

    amount_range_width_strip = (999 / 1000)

    alerts_per_customer = 3
