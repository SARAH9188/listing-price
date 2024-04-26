import datetime
import numpy as np
import pyspark.sql.functions as F

try:
    from crs_prepipeline_tables import TRANSACTIONS, CDDALERTS, TMALERTS, ACCOUNTS, \
        CUSTOMERS, C2A, C2C, HIGHRISKPARTY, HIGHRISKCOUNTRY
    from crs_utils import table_checker
    from crs_postpipeline_tables import UI_CDDALERTS
except:
    import sys
    sys.path.append('.')
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS, CDDALERTS, TMALERTS, ACCOUNTS, \
        CUSTOMERS, C2A, C2C, HIGHRISKPARTY, HIGHRISKCOUNTRY
    from CustomerRiskScoring.src.crs_utils.crs_utils import table_checker
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_CDDALERTS


class ConfigRiskCRSPreprocess:
    def __init__(self):

        """
        ConfigRiskCRSPreprocess:
        All configurable variables and values for CRS risk indicators preprocessing should be declared here
        """

        # Minimal columns that are required for the module to run, without which it errors
        # TRANSACTIONS
        minimal_txn_cols = [TRANSACTIONS.transaction_key,
                            TRANSACTIONS.primary_party_key,
                            TRANSACTIONS.account_key,
                            TRANSACTIONS.txn_date_time,
                            TRANSACTIONS.txn_type_category,
                            TRANSACTIONS.txn_amount,
                            ]

        # TM_ALERTS or CDD_ALERTS
        minimal_alert_cols = [CDDALERTS.alert_id,
                              CDDALERTS.party_key,
                              CDDALERTS.alert_created_date,
                              CDDALERTS.alert_investigation_result,
                              ]

        # ANOMALY
        minimal_anomaly_cols = [UI_CDDALERTS.alert_id,
                                UI_CDDALERTS.primary_party_key,
                                UI_CDDALERTS.alert_created_date,
                                UI_CDDALERTS.investigation_result,
                                ]

        # ACCOUNTS
        minimal_account_cols = [ACCOUNTS.account_key]

        # CUSTOMERS
        minimal_party_cols = [CUSTOMERS.party_key]

        # HIGHRISKPARTY
        minimal_high_risk_party_cols = [HIGHRISKPARTY.party_key]

        # HIGHRISKCOUNTRY
        minimal_high_risk_country_cols = [HIGHRISKCOUNTRY.country_code]

        # C2A
        minimal_c2a_cols = [C2A.party_key, C2A.account_key]

        # configurable columns: below column list will be used to select the required data from the actual tables
        # TRANSACTIONS
        config_txn_cols = minimal_txn_cols + [TRANSACTIONS.opp_country_code]


        ### added above lines of code into the function table_checker in tm_utils.py
        self.txn_cols = table_checker(minimal_txn_cols, config_txn_cols, 'TRANSACTIONS')

        # TM_ALERTS or CDD_ALERTS
        config_alert_cols = minimal_alert_cols + []

        self.alert_cols = table_checker(minimal_alert_cols, config_alert_cols, 'ALERTS')

        # ANOMALY
        config_anomaly_cols = minimal_anomaly_cols + []

        self.anomaly_cols = table_checker(minimal_anomaly_cols, config_anomaly_cols, 'ANOMALY')

        # ACCOUNTS
        config_account_cols = minimal_account_cols + [ACCOUNTS.primary_party_key]

        self.account_cols = table_checker(minimal_account_cols, config_account_cols, 'ACCOUNTS')

        # CUSTOMERS
        config_party_cols = minimal_party_cols

        self.party_cols = table_checker(minimal_party_cols, config_party_cols, 'CUSTOMERS')

        # HIGHRISKPARTY
        config_high_risk_party_cols = minimal_high_risk_party_cols + []

        self.high_risk_party_cols = table_checker(minimal_high_risk_party_cols, config_high_risk_party_cols,
                                                  'HIGHRISKPARTY')

        # HIGHRISKCOUNTRY
        config_high_risk_country_cols = minimal_high_risk_country_cols + []

        self.high_risk_country_cols = table_checker(minimal_high_risk_country_cols, config_high_risk_country_cols,
                                                    'HIGHRISKCOUNTRY')

        # C2A
        config_c2a_cols = minimal_c2a_cols + []

        self.c2a_cols = table_checker(minimal_c2a_cols, config_c2a_cols, 'C2A')

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
        # self.alert_assoc_account_key = self.assoc_string + TMALERTS.account_key
        self.alert_assoc_alert_date = self.assoc_string + CDDALERTS.alert_created_date

        # target variable encoding
        self.target_mapping = {0: ["Non-STR"],
                               1: ["STR"]}


class ConfigCrsRiskFeatureBox:
    def __init__(self, feature_map=None):
        '''
        ConfigCRSFeatureBox:
        All configurable variables and values for CRSFeatureBox should be declared here
        :param feature_map: feature map contains 'TRANSACTIONS & ALERTS' table with base columns and derived columns
                            mapping which acts as the base for the FeatureBox module
        '''

        self.feature_map = feature_map

        # used for Unsupervised FeatureBox only
        self.today = datetime.datetime.today()


        # string variable
        transaction_table = 'TRANSACTIONS'
        alert_table = 'ALERTS'

        # column variables for TRANSACTIONS - type: String
        self.txn_alert_id_col = self.feature_map.get(transaction_table).get('txn_alert_id')
        self.txn_alert_date_col = self.feature_map.get(transaction_table).get('txn_alert_date')
        self.txn_account_key_col = self.feature_map.get(transaction_table).get('account_key')
        self.txn_assoc_account_key_col = self.feature_map.get(transaction_table).get('assoc_account_key')
        self.txn_party_key_col = self.feature_map.get(transaction_table).get('party_key')
        self.txn_key_col = self.feature_map.get(transaction_table).get('transaction_key')
        self.txn_date_col = self.feature_map.get(transaction_table).get('transaction_date')
        self.txn_category_col = self.feature_map.get(transaction_table).get('transaction_category')
        self.txn_amount_col = self.feature_map.get(transaction_table).get('transaction_amount')
        self.compare_party_col = self.feature_map.get(transaction_table).get('compare_both_party')
        self.foreign_country_col = self.feature_map.get(transaction_table).get('foreign_country_ind')
        self.high_risk_country_col = self.feature_map.get(transaction_table).get('high_risk_country_ind')
        self.high_risk_party_col = self.feature_map.get(transaction_table).get('high_risk_party_ind')
        self.compare_account_bu_col = self.feature_map.get(transaction_table).get('compare_account_business_unit')
        self.compare_account_type_col = self.feature_map.get(transaction_table).get('compare_account_type_code')
        self.compare_account_segment_col = self.feature_map.get(transaction_table).get('compare_account_segment_code')
        self.compare_party_type_col = self.feature_map.get(transaction_table).get('compare_party_type_code')
        self.pep_col = self.feature_map.get(transaction_table).get('pep_ind')

        # column variables for ALERTS - type: String
        self.alert_id_col = self.feature_map.get(alert_table).get('alert_id')
        self.alert_date_col = self.feature_map.get(alert_table).get('alert_date')
        self.history_alert_date_col = self.feature_map.get(alert_table).get('history_alert_date')
        self.alert_assoc_alert_date_col = self.feature_map.get(alert_table).get('assoc_alert_date')
        self.alert_account_key_col = self.feature_map.get(alert_table).get('account_key')
        self.alert_assoc_account_key_col = self.feature_map.get(alert_table).get('assoc_account_key')
        self.alert_party_key_col = self.feature_map.get(alert_table).get('party_key')
        self.alert_status_col = self.feature_map.get(alert_table).get('alert_status')

        # filter variables on TRANSACTIONS or related table
        # transaction_lookback_days
        self.txn_lookback_filter = {"DAILY": 1,
                                    "7DAY": 7,
                                    "30DAY": 30,
                                    "90DAY": 90,
                                    "180DAY": 180,
                                    "360DAY": 360}

        # trasaction_lookback_days - monthly
        txn_monthly_lookback_days = [90, 180, 360]
        self.txn_monthly_lookback_filter = {key: value for key, value in self.txn_lookback_filter.items()
                                            if value in txn_monthly_lookback_days}

        txngap_days = [30, 90, 180]
        self.txngap_lookback_filter = {key: value for key, value in self.txn_lookback_filter.items()
                                       if value in txngap_days}

        # txn_gap_lookback_days
        txngap_days = [30, 90, 180]
        self.txngap_lookback_filter = {key: value for key, value in self.txn_lookback_filter.items()
                                       if value in txngap_days}

        # txn_offset_days
        offset_exclude_days = [1, 360]
        self.txn_offset_filter = {key: value for key, value in self.txn_lookback_filter.items()
                                  if value not in offset_exclude_days}

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
            "incoming-fund-transfer": ["incoming-local-fund-transfer", "incoming-overseas-fund-transfer"],
            "outgoing-fund-transfer": ["outgoing-local-fund-transfer", "outgoing-overseas-fund-transfer"],
            'high-risk-incoming': ["CDM-cash-deposit", "cash-equivalent-deposit", "cash-equivalent-card-payment",
                                   "incoming-local-fund-transfer", 'incoming-overseas-fund-transfer'],
            'high-risk-outgoing': ["ATM-withdrawal", "cash-equivalent-withdrawal",
                                   "outgoing-local-fund-transfer", 'outgoing-overseas-fund-transfer'],
            "incoming-all": ["CDM-cash-deposit", "cash-equivalent-deposit",
                             "incoming-cheque", "cash-equivalent-card-payment",
                             "card-payment", "incoming-local-fund-transfer",
                             "incoming-overseas-fund-transfer", "CDM-cash-deposit",
                             "Misc-credit"],
            "outgoing-all": ["ATM-withdrawal", "cash-equivalent-withdrawal",
                             "card-charge", "outgoing-cheque", "outgoing-local-fund-transfer",
                             "outgoing-overseas-fund-transfer", "Misc-debit"]
        }

        # credit_or_debit mapping
        credit_debit = ["incoming-all", "outgoing-all"]
        self.credit_debit_filter = {key: value for key, value in self.txn_category_filter.items() if
                                    key in credit_debit}

        # transfer_credit_or_debit for fund transfer (account to account)
        transfer_credit_debit = ["incoming-fund-transfer", "outgoing-fund-transfer"]
        self.trf_credit_debit_filter = {key: value for key, value in self.txn_category_filter.items()
                                        if key in transfer_credit_debit}

        # structured typology to look for transactions within a range or beyond a range
        self.structured_txn_filter = {"45kto50k": [45000.0, 50000.0], ">50k": [(50000.0 + 1e-10), np.inf]}

        # indicator to compute transaction gap features
        self.txngap_filter = {"TXN-GAP": True}

        # indicator to filter transfers to high risk countries
        self.risk_country_filter = {"TO-HIGH-RISK-COUNTRY": True}

        # indicator to filter transfers to high risk parties
        self.risk_party_filter = {"TO-HIGH-RISK-PARTY": True}

        # indicator to filter transfers to diff party in bank and non-bank customers
        self.same_party_trf_filter = {"DIFF-PARTY": False}

        # indicator to filter transfers to foreign country
        self.foreign_country_filter = {"TO-FOREIGN": True}

        # indicator to filter transfers which has diff account bu for source and dest accounts
        self.account_bu_filter = {"DIFF-ACCT-BU": True}

        # indicator to filter transfers which has diff account type for source and dest accounts
        self.account_type_filter = {"DIFF-ACCT-TYPE": True}

        # indicator to filter transfers which has diff account segment for source and dest accounts
        self.account_segment_filter = {"DIFF-ACCT-SEGMENT": True}

        # indicator to filter transfers which has diff party type for source and dest parties
        self.party_type_filter = {"DIFF-PARTY-TYPE": True}

        # indicator to filter transfers to PEP party or account
        self.pep_filter = {"WITH-PEP": True}

        # filter variables on ALERTS or related table
        # alert_lookback_days
        self.alert_lookback_filter = {"15DAY": 15,
                                      "30DAY": 30,
                                      "90DAY": 90,
                                      "180DAY": 180,
                                      "360DAY": 360}

        # target lookback days
        self.target_lookback_filter = {"360DAY": 360}
        # alert_offset_days

        self.alert_offset_filter = {key: value for key, value in self.alert_lookback_filter.items()
                                    if value not in offset_exclude_days}
        # self.alert_offset_filter = {'15DAY': 15, '30DAY': 30, '90DAY': 90, '180DAY': 180}

        # false_alert_possible_values
        self.false_alert_filter = {"FSTR": 0}

        # true_alert_possible_values
        self.true_alert_filter = {"STR": 1}

        # operation filters
        # operation - sum, count, avg, max, stddev
        self.operation_filter = {"AMT": F.sum, "VOL": F.count, "AVG": F.mean, "MAX": F.max, "SD": F.stddev}
        # operation - sum  only
        self.operation_sum_filter = {"AMT": F.sum}
        # operation - count only
        self.operation_vol_filter = {"COUNT": F.count}
        # operation - avg only
        self.operation_avg_filter = {"AVG": F.mean}
        # operation - max only
        self.operation_max_filter = {"MAX": F.max}
        # operation - sd only
        self.operation_sd_filter = {"SD": F.stddev}
        # operation - stddev, avg only
        self.operation_avg_sd_filter = {"AVG": F.mean, "SD": F.stddev}
        # operation - sum, count, avg and without max, stddev
        self.operation_wo_sd_max_filter = {"AMT": F.sum, "VOL": F.count, "AVG": F.mean}
        # operation - sum, count, avg, max and with stddev
        self.operation_wo_sd_filter = {"AMT": F.sum, "VOL": F.count, "AVG": F.mean, "MAX": F.max}

        # string constants
        self.alert_string = "ALERT"

        # filters for second level aggreation features
        # in and out txn category mapping for net operation
        net_txn_mapping = {'CDM-cash-deposit': ['ATM-withdrawal'],
                           'cash-equivalent-deposit': ['cash-equivalent-withdrawal'],
                           'incoming-cheque': ['outgoing-cheque'],
                           'incoming-local-fund-transfer': ['outgoing-local-fund-transfer'],
                           'incoming-overseas-fund-transfer': ['outgoing-overseas-fund-transfer'],
                           'incoming-fund-transfer': ['outgoing-fund-transfer'],
                           'incoming-all': ['outgoing-all']}

        # ensure if all the mapping is present in the base transaction category mapping, if not available, it will be
        # skipped.
        self.net_txn_mapping = {}
        for key, value_list in net_txn_mapping.items():
            if key in list(self.txn_category_filter.keys()):
                new_value_list = []
                for value in value_list:
                    if value in list(self.txn_category_filter.keys()):
                        new_value_list.append(value)
                self.net_txn_mapping.update({key: new_value_list})

        # self.net_txn_mapping = {key: value_list for key, value_list in net_txn_mapping.items()
        #                         for value in value_list
        #                         if key in list(self.txn_category_filter.keys()) and
        #                         value in list(self.txn_category_filter.keys())}

        # first level operations that needed to be considered for net operation eg: sum
        self.net_operation = [x for x, y in self.operation_sum_filter.items()]

        # in and out txn category mapping for ratio (flux) operation
        divide_txn_mapping = {'CDM-cash-deposit': ['ATM-withdrawal', "incoming-all"],
                              'cash-equivalent-deposit': ['cash-equivalent-withdrawal'],
                              'incoming-cheque': ['outgoing-cheque'],
                              'incoming-local-fund-transfer': ['outgoing-local-fund-transfer'],
                              'incoming-overseas-fund-transfer': ['outgoing-overseas-fund-transfer'],
                              'incoming-fund-transfer': ['outgoing-fund-transfer'],
                              'incoming-all': ['outgoing-all']}

        # ensure if all the mapping is present in the base transaction category mapping, if not available, it will be
        # skipped.
        self.divide_txn_mapping = {}
        for key, value_list in divide_txn_mapping.items():
            if key in list(self.txn_category_filter.keys()):
                new_value_list = []
                for value in value_list:
                    if value in list(self.txn_category_filter.keys()):
                        new_value_list.append(value)
                self.divide_txn_mapping.update({key: new_value_list})

        # in and out txn category mapping for flow through adj operation
        self.flow_through_adj_txn_mapping = {'cash-equivalent-withdrawal': ['incoming-all'],
                                             'outgoing-fund-transfer': ['incoming-all', 'incoming-fund-transfer'],
                                             'outgoing-all': ['incoming-all', 'cash-equivalent-deposit',
                                                              'incoming-fund-transfer']
                                             }

        # ensure if all the mapping is present in the base transaction category mapping, if not available, it will be
        # skipped.
        self.flow_through_adj_txn_mapping = {key: value_list for key, value_list in
                                             self.flow_through_adj_txn_mapping.items()
                                             for value in value_list
                                             if key in list(self.txn_category_filter.keys()) and
                                             value in list(self.txn_category_filter.keys())}

        # first level operations that needed to be considered for ratio (flux) operation eg: sum
        self.divide_operation = [x for x, y in self.operation_sum_filter.items()]
        self.flow_through_adj_operation = [x for x, y in self.operation_sum_filter.items()]

        # day mapping for historical background comparison (hbc)
        self.hbc_day_mapping = {'DAILY': ['7DAY', '30DAY', '90DAY', '180DAY', '360DAY'],
                                '7DAY': ['30DAY', '90DAY', '180DAY', '360DAY'],
                                '30DAY': ['90DAY', '180DAY', '360DAY'],
                                '90DAY': ['180DAY', '360DAY'],
                                '180DAY': ['360DAY']
                                }
        # first level operations that needed to be considered for hbc operation eg: sum
        self.hbc_operation = list(self.operation_wo_sd_max_filter.keys())

        # day mapping for offset comparison (short duration: list(long durations))
        self.offset_day_mapping = {
            '7DAY': ['30DAY', '90DAY', '180DAY', '360DAY'],
            '30DAY': ['90DAY', '180DAY', '360DAY'],
            '90DAY': ['180DAY', '360DAY'],
            '180DAY': ['360DAY']
        }

        # used for avg_vs_avg_offset and max_vs_avg_offset
        self.avg_operation_string = list(self.operation_avg_filter.keys())[0]

        # used for max_vs_avg_offset
        self.max_operation_string = list(self.operation_max_filter.keys())[0]

        # used for false_alert vs all_alerts operation
        self.fstr_vs_alert_operation = {list(self.false_alert_filter.keys())[0]: self.alert_string}

        # string columns to be used in typology mapping data
        self.typology_string_columns = ['feature_name', 'level']

        # below portion is for the risk indicators
        self.risk_indicators_list = [
                                "CDM-cash-deposit_30DAY_AMT",
                                "ATM-withdrawal_30DAY_AMT",
                                "CDM-cash-deposit_30DAY_VOL",
                                "ATM-withdrawal_30DAY_VOL",
                                "incoming-overseas-fund-transfer_30DAY_AMT",
                                "outgoing-overseas-fund-transfer_30DAY_AMT",
                                "flow-through-adj_outgoing-fund-transfer_30DAY_AMT_incoming-fund-transfer_30DAY_AMT",
                                "incoming-fund-transfer_30DAY_TO-HIGH-RISK-COUNTRY_AMT",
                                "outgoing-fund-transfer_30DAY_TO-HIGH-RISK-COUNTRY_AMT",
                                "card-payment_90DAY_AVG",
                                "card-payment_180DAY_AVG",
                                "ratio_incoming-all_30DAY_AVG_incoming-all_180DAY_OFFSET_30DAY_AVG",
                                "ratio_outgoing-all_30DAY_AVG_outgoing-all_180DAY_OFFSET_30DAY_AVG",
                                "ratio_incoming-all_30DAY_AMT_incoming-all_90DAY_AMT",
                                "ratio_outgoing-all_30DAY_AMT_outgoing-all_90DAY_AMT"
                                ]

        self.two_level_operation = ['ratio', 'net', 'flow-through-adj']

        self.list_of_operations = list(self.operation_filter) + self.two_level_operation

        self.risk_indicator_lookback_days = [0,30,60,90,120,150]

RANDOM_NUM_RANGE = 10**4
TXN_COUNT_THRESHOLD_SALT = 10**5
SALT_REPARTION_NUMBER = 10**4
INTERNAL_BROADCAST_LIMIT_MB = 700.0
