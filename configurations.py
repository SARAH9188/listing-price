try:
    from prepipeline_tables import TRANSACTIONS, TMALERTS, ACCOUNTS, CUSTOMERS, HIGHRISKCOUNTRY, HIGHRISKPARTY, C2A, C2C
    from postpipeline_tables import UI_TMALERTS
    from tm_utils import table_checker
except:
    from TransactionMonitoring.src.tm_feature_engineering.prepipeline_tables import TRANSACTIONS, TMALERTS, ACCOUNTS, \
        CUSTOMERS, HIGHRISKCOUNTRY, HIGHRISKPARTY, C2A, C2C
    from TransactionMonitoring.src.tm_ui_mapping.postpipeline_tables import UI_TMALERTS
    from TransactionMonitoring.src.tm_utils.tm_utils import table_checker

import datetime
import numpy as np
import pyspark.sql.functions as F


class ConfigStaticFeatures:
    def __init__(self):
        '''
        ConfigStaticFeatures:
        All configurable variables and values for StaticFeatures should be declared here
        '''

        # configurable columns that needed to be picked as part of static features for ACCOUNTS
        self.account_cols = [ACCOUNTS.account_key,
                             ACCOUNTS.status_code,
                             ACCOUNTS.open_date,
                             ACCOUNTS.type_code,
                             ACCOUNTS.segment_code,
                             ACCOUNTS.business_unit,
                             ACCOUNTS.mail_code,
                             ACCOUNTS.undelivered_mail_flag,
                             ACCOUNTS.sector_code,
                             ACCOUNTS.currency_code,
                             ACCOUNTS.closed_date,
                             ACCOUNTS.credit_limit,
                             ]

        # checks if column variable has proper column name assigned to it from the TABLES class - ACCOUNTS
        self.account_cols = [x for x in self.account_cols if x is not None]

        # configurable columns that needed to be picked as part of static features for CUSTOMERS
        self.party_cols = [CUSTOMERS.party_key,
                           CUSTOMERS.individual_corporate_type,
                           CUSTOMERS.date_of_birth_or_incorporation,
                           CUSTOMERS.entity_type,
                           CUSTOMERS.customer_segment_code,
                           CUSTOMERS.citizenship_country,
                           CUSTOMERS.domicile_country,
                           CUSTOMERS.residence_operation_country,
                           CUSTOMERS.birth_incorporation_country,
                           CUSTOMERS.country_of_origin,
                           CUSTOMERS.gender,
                           CUSTOMERS.race,
                           CUSTOMERS.marital_status,
                           CUSTOMERS.customer_segment_code,
                           CUSTOMERS.business_type,
                           CUSTOMERS.occupation,
                           CUSTOMERS.employment_status,
                           CUSTOMERS.risk_score,
                           CUSTOMERS.risk_level,
                           CUSTOMERS.pep_flag,
                           CUSTOMERS.pep_not_on_watchlist,
                           CUSTOMERS.pep_by_account_association,
                           CUSTOMERS.employee_flag,
                           CUSTOMERS.foreign_financial_org_flag,
                           CUSTOMERS.foreign_official_flag,
                           CUSTOMERS.non_physical_address_flag,
                           CUSTOMERS.residence_flag,
                           CUSTOMERS.special_attention_flag,
                           CUSTOMERS.incorporate_taxhaven_flag,
                           CUSTOMERS.bankrupt_flag,
                           CUSTOMERS.near_border_flag,
                           CUSTOMERS.adverse_news_flag,
                           CUSTOMERS.weak_aml_ctrl_flag,
                           CUSTOMERS.cash_intensive_business_flag,
                           CUSTOMERS.correspondent_bank_flag,
                           CUSTOMERS.money_service_bureau_flag,
                           CUSTOMERS.non_bank_finance_institute_flag,
                           CUSTOMERS.wholesale_banknote_flag,
                           CUSTOMERS.compensation_reqd_flag,
                           CUSTOMERS.complaint_flag,
                           CUSTOMERS.end_relationship_flag,
                           CUSTOMERS.face_to_face_flag,
                           CUSTOMERS.ngo_flag,
                           CUSTOMERS.high_risk_country_flag,
                           CUSTOMERS.balance_sheet_total,
                           CUSTOMERS.annual_turnover,
                           CUSTOMERS.trading_duration
                           ]

        # checks if column variable has proper column name assigned to it from the TABLES class - CUSTOMERS
        self.party_cols = [x for x in self.party_cols if x is not None]

        # configurable columns that needed to be picked as part of static features for C2C
        self.c2c_cols = [C2C.party_key,
                         C2C.linked_party_key,
                         C2C.relation_code,
                         C2C.relationship_end_date]

        # checks if column variable has proper column name assigned to it from the TABLES class - C2C
        self.c2c_cols = [x for x in self.c2c_cols if x is not None]

        # configurable columns that needed to be picked as part of static features for C2A
        self.c2a_cols = [C2A.party_key,
                         C2A.account_key,
                         C2A.relation_code]

        # checks if column variable has proper column name assigned to it from the TABLES class - C2A
        self.c2a_cols = [x for x in self.c2a_cols if x is not None]

        # configurable columns that needed to be picked as part of static features for ALERTS
        self.alert_cols = [TMALERTS.alert_id,
                           TMALERTS.alert_created_date,
                           TMALERTS.party_key,
                           TMALERTS.alert_investigation_result]

        # checks if column variable has proper column name assigned to it from the TABLES class - ALERTS
        self.alert_cols = [x for x in self.alert_cols if x is not None]

        # configurable columns that needed to be picked as part of static features from HIGH RISK PARTY
        self.high_risk_party_cols = [HIGHRISKPARTY.party_key,
                                     HIGHRISKPARTY.high_risk_start_date,
                                     HIGHRISKPARTY.high_risk_expiry_date]

        # checks if column variable has proper column name assigned to it from the TABLES class - HIGH RISK PARTY
        self.high_risk_party_cols = [x for x in self.high_risk_party_cols if x is not None]

        # variable for current date as some static features are dependent on current date eg: age
        self.today = datetime.datetime.today()

        # target variable encoding
        self.target_mapping = {0: ["Non-STR","0"],
                               1: ["STR","1"]}


class ConfigTMFeatureBox:
    def __init__(self, feature_map=None):
        '''
        ConfigTMFeatureBox:
        All configurable variables and values for TMFeatureBox should be declared here
        :param feature_map: feature map contains 'TRANSACTIONS & ALERTS' table with base columns and derived columns
                            mapping which acts as the base for the FeatureBox module
        '''

        self.feature_map = feature_map

        # used for Unsupervised FeatureBox only
        self.today = datetime.datetime.today()


        # string variable
        transaction_table = 'TRANSACTIONS'
        alert_table = 'ALERTS'

        if feature_map != None:
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
            'high-risk-incoming': ["CDM-cash-deposit",
                                   "cash-equivalent-deposit", "cash-equivalent-card-payment",
                                   "incoming-local-fund-transfer", 'incoming-overseas-fund-transfer'],
            'high-risk-outgoing': ["ATM-withdrawal", "cash-equivalent-withdrawal",
                                   "outgoing-local-fund-transfer", 'outgoing-overseas-fund-transfer'],
            "incoming-all": ["CDM-cash-deposit", "cash-equivalent-deposit", "incoming-cheque","card-payment",
                             "cash-equivalent-card-payment", "incoming-local-fund-transfer", "incoming-overseas-fund-transfer",
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
        self.structured_txn_filter = {"45kto49k": [45000.0, 50000.0], ">50k": [50000.0, np.inf]}

        # indicator to compute transaction gap features
        self.txngap_filter = {"TXN-GAP": True}

        # indicator to filter transfers to high risk countries
        self.risk_country_filter = {"TO-HIGH-RISK-COUNTRY": True}

        # indicator to filter transfers to high risk parties
        self.risk_party_filter = {"TO-HIGH-RISK-PARTY": True}

        # indicator to filter transfers to same party in bank and non-bank customers
        self.same_party_trf_filter = {"SAME-PARTY": True}

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
        self.operation_alert_vol_filter = {"COUNT": F.count}
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

# to override the pipeline data if the data is past data in POC
PIPELINE_OVERRIDE_TODAY_DATE = None

TRAIN_SUPERVISED_MODE = "train"
PREDICT_SUPERVISED_MODE = "predict"
SUPERVISED_ASSOCIATED_MODE = "supervised"
ANOMALY_ASSOCIATED_MODE = "anomaly"
UNSUPERVISED_DATE_COL = 'CREATE_DATE'
UNION_BRANCH_CLUSTER_REMOVE_COLUMNS = ['_modelJobId_cluster']
INTERNAL_BROADCAST_LIMIT_MB = 700.0


class ErrorCodes:
    # error codes
    TM_ERR_A100 = "TM_ERR_A100"
    TM_ERR_C100 = "TM_ERR_C100"
    TM_ERR_T100 = "TM_ERR_T100"
    TM_ERR_U100 = "TM_ERR_U100"
