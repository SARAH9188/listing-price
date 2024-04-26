import datetime
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

try:
    from crs_prepipeline_tables import TRANSACTIONS, ACCOUNTS, CUSTOMERS, C2A, C2C, HIGHRISKCOUNTRY, HIGHRISKPARTY, \
        TMALERTS, CDDALERTS
    from crs_utils import table_checker
    from crs_postpipeline_tables import UI_CDDALERTS
    from crs_intermediate_tables import ANOMALY_OUTPUT

except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables \
        import TRANSACTIONS, ACCOUNTS, CUSTOMERS, C2A, C2C, HIGHRISKCOUNTRY, HIGHRISKPARTY, TMALERTS, CDDALERTS
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_CDDALERTS
    from CustomerRiskScoring.src.crs_utils.crs_utils import table_checker
    from CustomerRiskScoring.tables.crs_intermediate_tables import ANOMALY_OUTPUT


class ConfigCRSRulesEngine:
    def __init__(self):
        """
        ConfigCRSPreprocess:
        All configurable variables and values for CRS semi supervised preprocessing should be declared here
        """

        # Minimal columns that are required for the module to run, without which it errors
        # TRANSACTIONS
        self.minimal_txn_cols = [TRANSACTIONS.primary_party_key,
                                 TRANSACTIONS.txn_date_time,
                                 TRANSACTIONS.txn_type_category,
                                 TRANSACTIONS.txn_amount,
                                 ]

        # TM_ALERTS or CDD_ALERTS
        self.minimal_alert_cols = [CDDALERTS.alert_id,
                                   CDDALERTS.party_key,
                                   CDDALERTS.alert_created_date,
                                   CDDALERTS.alert_investigation_result,
                                   ]


        # ACCOUNTS
        self.minimal_account_cols = [ACCOUNTS.account_key]

        # CUSTOMERS
        self.minimal_party_cols = [CUSTOMERS.party_key]

        # HIGHRISKPARTY
        self.minimal_high_risk_party_cols = [HIGHRISKPARTY.party_key]

        # HIGHRISKCOUNTRY
        self.minimal_high_risk_country_cols = [HIGHRISKCOUNTRY.country_code]

        # C2A
        self.minimal_c2a_cols = [C2A.party_key, C2A.account_key]

        # configurable columns: below column list will be used to select the required data from the actual tables
        # TRANSACTIONS
        self.config_txn_cols = self.minimal_txn_cols + \
                               [TRANSACTIONS.country_code,
                                TRANSACTIONS.opp_country_code] + \
                               [TRANSACTIONS.account_key, TRANSACTIONS.opp_account_key, TRANSACTIONS.opp_account_number]
        # new columns for new features

        # TM_ALERTS or CDD_ALERTS
        self.config_alert_cols = self.minimal_alert_cols + []

        # ACCOUNTS
        self.config_account_cols = self.minimal_account_cols + [ACCOUNTS.primary_party_key] + [ACCOUNTS.business_unit]
        # new columns for new features

        # CUSTOMERS
        self.config_party_cols = self.minimal_party_cols + [CUSTOMERS.individual_corporate_type, CUSTOMERS.pep_flag,
                                                            CUSTOMERS.adverse_news_flag]
        # new columns for new features

        # HIGHRISKPARTY
        self.config_high_risk_party_cols = self.minimal_high_risk_party_cols + []

        # HIGHRISKCOUNTRY
        self.config_high_risk_country_cols = self.minimal_high_risk_country_cols + []

        # C2A
        self.config_c2a_cols = self.minimal_c2a_cols + []

        # assigning variables for the newly created columns inside the module for reusability
        self.txn_opp_party_key = "OPP_" + TRANSACTIONS.primary_party_key
        self.txn_opp_party_type_code = "OPP_" + CUSTOMERS.individual_corporate_type
        self.txn_opp_account_type_code = "OPP_" + ACCOUNTS.type_code
        self.txn_opp_account_segment_code = "OPP_" + ACCOUNTS.segment_code
        self.txn_opp_account_business_unit = "OPP_" + ACCOUNTS.business_unit
        self.txn_to_foreign_country = "FOREIGN_COUNTRY_IND"
        self.txn_to_high_risk_party = "HIGH_RISK_PARTY_IND"
        self.txn_to_high_risk_country = "HIGH_RISK_COUNTRY_IND"
        self.txn_opp_party_pep_flag = "PEP"
        self.same_compare_string = "SAME_"
        self.diff_compare_string = "DIFF_"
        self.alert_history_string = "HISTORY_"
        self.assoc_string = "ASSOC_"
        self.txn_compare_account_business_unit = self.diff_compare_string + ACCOUNTS.business_unit
        self.txn_compare_account_type_code = self.diff_compare_string + ACCOUNTS.type_code
        self.txn_compare_account_segment_code = self.diff_compare_string + ACCOUNTS.segment_code
        self.txn_compare_party_type_code = self.diff_compare_string + CUSTOMERS.individual_corporate_type
        self.history_alert_date = self.alert_history_string + CDDALERTS.alert_created_date
        self.txn_same_party_transfer = self.same_compare_string + "PARTY_TRANSFER"
        # self.alert_assoc_account_key = self.assoc_string + CDDALERTS.account_key
        self.alert_assoc_alert_date = self.assoc_string + CDDALERTS.alert_created_date
        self.txn_opp_account_key = TRANSACTIONS.opp_account_key
        self.txn_opp_account_number = "OPP_" + ACCOUNTS.account_number
        self.txn_opp_account_key_num = "OPP_ACC_KEY_NUM"
        self.txn_opp_party_adverse_news_flag = "OPP_ADVERSE_NEWS_IND"

        # target variable encoding
        self.target_mapping = {0: ["Non-STR", "False-Alert", "0"],
                               1: ["STR", "True-Alert", "1"]}


class ConfigCRSRulesEngineFeatureBox:
    def __init__(self, feature_map=None):
        """
        ConfigCRSFeatureBox:
        All configurable variables and values for CRSFeatureBox should be declared here
        :param feature_map: feature map contains 'TRANSACTIONS & ALERTS' table with base columns and derived columns
                            mapping which acts as the base for the FeatureBox module
        """

        self.feature_map = feature_map

        # used for Unsupervised FeatureBox only
        self.today = datetime.datetime.today()

        # string variable
        transaction_table = 'TRANSACTIONS'
        alert_table = 'ALERTS'

        # transaction_category - mapping
        self.txn_category_filter = {
            "ATM-withdrawal": ["ATM-withdrawal"],
            "CDM-cash-deposit": ["CDM-cash-deposit"],
            "cash-equivalent-deposit": ["cash-equivalent-deposit"],
            "cash-equivalent-withdrawal": ["cash-equivalent-withdrawal"],
            "cash-equivalent-card-payment": ["cash-equivalent-card-payment"],
            "card-payment": ["card-payment"],
            "card-charge": ["card-charge"],
            "incoming-cheque": ["incoming-cheque"],
            "outgoing-cheque": ['outgoing-cheque'],
            "incoming-local-fund-transfer": ["incoming-local-fund-transfer"],
            "outgoing-local-fund-transfer": ["outgoing-local-fund-transfer"],
            "incoming-overseas-fund-transfer": ['incoming-overseas-fund-transfer'],
            "outgoing-overseas-fund-transfer": ['outgoing-overseas-fund-transfer'],
            'Misc-credit': ['Misc-credit'],
            'Misc-debit': ['Misc-debit'],
            "incoming-fund-transfer": ["incoming-local-fund-transfer", "incoming-overseas-fund-transfer"],
            "outgoing-fund-transfer": ["outgoing-local-fund-transfer", "outgoing-overseas-fund-transfer"],
            'high-risk-incoming': ["CDM-cash-deposit",
                                   "cash-equivalent-deposit", "cash-equivalent-card-payment",
                                   "incoming-local-fund-transfer", 'incoming-overseas-fund-transfer'],
            'high-risk-outgoing': ["ATM-withdrawal", "cash-equivalent-withdrawal",
                                   "outgoing-local-fund-transfer", 'outgoing-overseas-fund-transfer'],
            "incoming-all": ["CDM-cash-deposit", "cash-equivalent-deposit",
                             "incoming-cheque", "cash-equivalent-card-payment",
                             "card-payment", "incoming-local-fund-transfer",
                             "incoming-overseas-fund-transfer",
                             "Misc-credit"],
            "outgoing-all": ["ATM-withdrawal", "cash-equivalent-withdrawal",
                             "card-charge", "outgoing-cheque", "outgoing-local-fund-transfer",
                             "outgoing-overseas-fund-transfer", "Misc-debit"]
        }

        # operation filters
        # operation - sum, count, avg, max, stddev
        self.operation_filter = {"AMT": F.sum, "VOL": F.count, "AVG": F.mean, "MAX": F.max, "SD": F.stddev,
                                 "DISTINCT-COUNT": F.countDistinct}
        # operation - sum  only
        self.operation_sum_filter = {"AMT": F.sum}
        # operation - vol only
        self.operation_vol_filter = {"VOL": F.count}
        # operation - count only
        self.operation_count_filter = {"COUNT": F.count}
        # operation - avg only
        self.operation_avg_filter = {"AVG": F.mean}
        # operation - max only
        self.operation_max_filter = {"MAX": F.max}
        # operation - sd only
        self.operation_sd_filter = {"SD": F.stddev}
        # operation - sum, count, avg, max and with stddev
        self.operation_wo_sd_filter = {"AMT": F.sum, "VOL": F.count, "AVG": F.mean, "MAX": F.max, "COUNT": F.count}
        # distinct count filter
        self.distinct_count_filter = {"DISTINCT-COUNT": F.countDistinct}

        # string constants
        self.alert_string = "ALERT"
        # string constants
        self.cdd_alert_string = "CDD-ALERT"


RE_TRAIN_MODE = "train"
RE_PREDICT_MODE = "predict"
RE_DATE_COL = 'CREATE_DATE'
NUM_PARTITIONS = 10 ** 3
RANDOM_NUM_RANGE = 10 ** 4
TXN_COUNT_THRESHOLD_SALT = 10 ** 5
SALT_REPARTION_NUMBER = 10 ** 4
INTERNAL_BROADCAST_LIMIT_MB = 700.0
