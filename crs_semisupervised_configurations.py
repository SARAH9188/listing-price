import datetime
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

try:
    from crs_prepipeline_tables import TRANSACTIONS, ACCOUNTS, CUSTOMERS, C2A, C2C, HIGHRISKCOUNTRY, HIGHRISKPARTY, \
        TMALERTS, CDDALERTS
    from crs_postpipeline_tables import UI_CDDALERTS
    from crs_intermediate_tables import ANOMALY_OUTPUT
    from constants import Defaults

except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables \
        import TRANSACTIONS, ACCOUNTS, CUSTOMERS, C2A, C2C, HIGHRISKCOUNTRY, HIGHRISKPARTY, TMALERTS, CDDALERTS
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_CDDALERTS
    from CustomerRiskScoring.tables.crs_intermediate_tables import ANOMALY_OUTPUT
    from Common.src.constants import Defaults


class ConfigCRSPreprocess:
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

        # ANOMALY
        self.minimal_anomaly_cols = [UI_CDDALERTS.alert_id,
                                     UI_CDDALERTS.primary_party_key,
                                     UI_CDDALERTS.alert_created_date,
                                     UI_CDDALERTS.investigation_result,
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

        # ANOMALY
        self.config_anomaly_cols = self.minimal_anomaly_cols + []

        # ACCOUNTS
        self.config_account_cols = self.minimal_account_cols + [ACCOUNTS.primary_party_key] + [ACCOUNTS.business_unit,
                                                                                               ACCOUNTS.segment_code,
                                                                                               ACCOUNTS.type_code]
        # new columns for new features

        # CUSTOMERS
        self.config_party_cols = self.minimal_party_cols + [CUSTOMERS.individual_corporate_type, CUSTOMERS.pep_flag,
                                                            CUSTOMERS.ngo_flag,
                                                            CUSTOMERS.adverse_news_flag,
                                                            CUSTOMERS.incorporate_taxhaven_flag,
                                                            CUSTOMERS.date_of_birth_or_incorporation]
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
        self.txn_opp_party_tax_haven_flag = "INCORPORATE_TAX_HAVEN"
        self.txn_opp_party_age = "oppPartyAge"
        self.txn_opp_party_pep_flag = "PEP"
        self.txn_opp_party_ngo_flag = "NGO"
        self.txn_opp_date_of_birth = "OPP_" + CUSTOMERS.date_of_birth_or_incorporation
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


class ConfigStaticFeatures:
    def __init__(self):
        """
        ConfigStaticFeatures:
        All configurable variables and values for StaticFeatures should be declared here
        """

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
                           CUSTOMERS.customer_segment_name,
                           CUSTOMERS.periodic_review_flag,
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
                           CUSTOMERS.trading_duration,
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
        self.alert_cols = [CDDALERTS.alert_id,
                           CDDALERTS.alert_created_date,
                           CDDALERTS.alert_investigation_result]

        # checks if column variable has proper column name assigned to it from the TABLES class - ALERTS
        self.alert_cols = [x for x in self.alert_cols if x is not None]

        # variable for current date as some static features are dependent on current date eg: age
        self.today = datetime.datetime.today()

        # target variable encoding
        self.target_mapping = {0: ["Non-STR", "False-Alert", "0"],
                               1: ["STR", "True-Alert", "1"]}


class ConfigCRSSemiSupervisedFeatureBox:
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

        if feature_map is not None:
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
            self.compare_account_segment_col = self.feature_map.get(transaction_table).get(
                'compare_account_segment_code')
            self.compare_party_type_col = self.feature_map.get(transaction_table).get('compare_party_type_code')
            self.pep_col = self.feature_map.get(transaction_table).get('pep_ind')
            self.ngo_col = self.feature_map.get(transaction_table).get('ngo_ind')
            self.adverse_news_col = self.feature_map.get(transaction_table).get('adverse_news_ind')
            self.incorporate_tax_haven_col = self.feature_map.get(transaction_table).get('incorporate_tax_haven_ind')

            # column variables for ALERTS - type: String
            self.alert_id_col = self.feature_map.get(alert_table).get('alert_id')
            self.alert_date_col = self.feature_map.get(alert_table).get('alert_date')
            self.history_alert_date_col = self.feature_map.get(alert_table).get('history_alert_date')
            self.alert_assoc_alert_date_col = self.feature_map.get(alert_table).get('assoc_alert_date')
            self.alert_account_key_col = self.feature_map.get(alert_table).get('account_key')
            # self.alert_assoc_account_key_col = self.feature_map.get(alert_table).get('assoc_account_key')
            self.alert_party_key_col = self.feature_map.get(alert_table).get('party_key')
            self.alert_status_col = self.feature_map.get(alert_table).get('alert_status')
            self.cdd_alert_status_col = self.feature_map.get(alert_table).get('cdd_alert_status')

        # filter variables on TRANSACTIONS or related table
        # transaction_lookback_days
        self.txn_lookback_filter = {"7DAY": 7,
                                    "30DAY": 30,
                                    "90DAY": 90,
                                    "180DAY": 180,
                                    "360DAY": 360
                                    }

        # trasaction_lookback_days - monthly
        self.txn_monthly_lookback_days = [90, 180, 360]

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
            "outgoing-card-payment": ["outgoing-card-payment"],
            "incoming-card-payment": ["incoming-card-payment"],
            "outgoing-card": ["outgoing-card"],
            "incoming-card": ["incoming-card"],
            "incoming-cheque": ["incoming-cheque"],
            "outgoing-cheque": ['outgoing-cheque'],
            "incoming-local-fund-transfer": ["incoming-local-fund-transfer"],
            "outgoing-local-fund-transfer": ["outgoing-local-fund-transfer"],
            "incoming-overseas-fund-transfer": ['incoming-overseas-fund-transfer'],
            "outgoing-overseas-fund-transfer": ['outgoing-overseas-fund-transfer'],
            "incoming-e-wallet": ["incoming-e-wallet"],
            "outgoing-e-wallet": ["outgoing-e-wallet"],
            "incoming-loan-payment": ["incoming-loan-payment"],
            "outgoing-loan-payment": ["outgoing-loan-payment"],
            'Misc-credit': ['Misc-credit'],
            'Misc-debit': ['Misc-debit'],
            "incoming-fund-transfer": ["incoming-local-fund-transfer", "incoming-overseas-fund-transfer"],
            "outgoing-fund-transfer": ["outgoing-local-fund-transfer", "outgoing-overseas-fund-transfer"],
            "incoming-cash": ["CDM-cash-deposit", "cash-equivalent-deposit"],
            "outgoing-cash": ["ATM-withdrawal", "cash-equivalent-withdrawal"],
            'high-risk-incoming': ["CDM-cash-deposit",
                                   "cash-equivalent-deposit", "cash-equivalent-card-payment",
                                   "incoming-local-fund-transfer", 'incoming-overseas-fund-transfer'],
            'high-risk-outgoing': ["ATM-withdrawal", "cash-equivalent-withdrawal",
                                   "outgoing-local-fund-transfer", 'outgoing-overseas-fund-transfer'],
            "incoming-all": ["CDM-cash-deposit", "cash-equivalent-deposit",
                             "incoming-cheque", "cash-equivalent-card-payment",
                             "card-payment", "incoming-local-fund-transfer",
                             "incoming-overseas-fund-transfer",
                             "Misc-credit", "incoming-cash", "incoming-e-wallet", "incoming-loan-payment",
                             "incoming-card", "incoming-card-payment"],
            "outgoing-all": ["ATM-withdrawal", "cash-equivalent-withdrawal",
                             "card-charge", "outgoing-cheque", "outgoing-local-fund-transfer",
                             "outgoing-overseas-fund-transfer", "Misc-debit",
                             "outgoing-cash", "outgoing-e-wallet", "outgoing-loan-payment", "outgoing-card",
                             "outgoing-card-payment"]
        }

        # credit_or_debit mapping
        self.credit_debit = ["incoming-all", "outgoing-all"]
        self.credit_debit_filter = {key: value for key, value in self.txn_category_filter.items() if
                                    key in self.credit_debit}

        # transfer_credit_or_debit for fund transfer (account to account)
        transfer_credit_debit = ["incoming-fund-transfer", "outgoing-fund-transfer"]
        self.trf_credit_debit_filter = {key: value for key, value in self.txn_category_filter.items()
                                        if key in transfer_credit_debit}
        # indicator for specific
        self.age_condition_to_filter_key = Defaults.age_condition_to_filter_key
        self.age_condition_to_filter_value = Defaults.age_condition_to_filter_value
        self.age_condition_to_filter = {self.age_condition_to_filter_key: self.age_condition_to_filter_value}

        # structured typology to look for transactions within a range or beyond a range
        self.structured_txn_filter = {"45kto50k": [45000.0, 50000.0], ">50k": [(50000.0 + 1e-10), np.inf]}

        # indicator to compute transaction gap features
        self.txngap_filter = {"TXN-GAP": True}

        # indicator to filter transfers to high risk countries
        self.risk_country_filter = {"TO-HIGH-RISK-COUNTRY": True}

        # indicator to filter transfers to high risk parties
        self.risk_party_filter = {"TO-HIGH-RISK-PARTY": True}

        # indicator to filter transfers to diff party in bank and non-bank customers
        self.same_party_trf_filter = {"SAME-PARTY": False}
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
        self.ngo_filter = {"WITH-NGO": True}
        # indicator to filter transfers to ADVERSE NEWS party or account
        self.adverse_news_filter = {"WITH-ADVERSE-NEWS": True}
        self.incorporate_tax_haven_filter = {"WITH-TAX-HAVEN": True}

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

        # false_alert_possible_values
        self.cdd_false_alert_filter = {"CDD-FSTR": 0}

        # true_alert_possible_values
        self.true_alert_filter = {"STR": 1}

        # true_alert_possible_values
        self.cdd_true_alert_filter = {"CDD-STR": 1}

        # operation filters
        # operation - sum, count, avg, max, stddev
        self.operation_filter = {"AMT": F.sum, "VOL": F.count, "AVG": F.mean, "MAX": F.max, "SD": F.stddev,
                                 "DISTINCT-COUNT": F.countDistinct}
        # self.operation_filter = {"AMT": F.sum, "VOL": F.count, "MAX": F.max, "AVG": F.mean}
        # operation - sum, count, max, stddev
        self.operation_filter_wo_avg_sd = {"AMT": F.sum, "VOL": F.count, "MAX": F.max}
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
        # operation - stddev, avg only
        self.operation_avg_sd_filter = {"AVG": F.mean, "SD": F.stddev}
        # operation - sum, count, avg and without max, stddev
        self.operation_wo_sd_max_filter = {"AMT": F.sum, "VOL": F.count, "AVG": F.mean}
        # operation - sum, count, avg, max and with stddev
        self.operation_wo_sd_filter = {"AMT": F.sum, "VOL": F.count, "AVG": F.mean, "MAX": F.max}
        # distinct count filter
        self.distinct_count_filter = {"DISTINCT-COUNT": F.countDistinct}

        # string constants
        self.alert_string = "ALERT"
        # string constants
        self.cdd_alert_string = "CDD-ALERT"

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
        self.avg_txn_mapping = {'AMT': ['VOL']}
        self.net_string = 'net'
        self.ratio_string = 'ratio'

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
        # self.hbc_day_mapping = {'DAILY': ['7DAY', '30DAY', '90DAY', '180DAY', '360DAY'],
        #                         '7DAY': ['30DAY', '90DAY', '180DAY', '360DAY'],
        #                         '30DAY': ['90DAY', '180DAY', '360DAY'],
        #                         '90DAY': ['180DAY', '360DAY'],
        #                         '180DAY': ['360DAY']
        #                         }
        self.hbc_day_mapping = {
            '30DAY': ['90DAY'],
        }
        # first level operations that needed to be considered for hbc operation eg: sum
        self.hbc_operation = list(self.operation_wo_sd_max_filter.keys())

        # day mapping for offset comparison (short duration: list(long durations))
        # self.offset_day_mapping = {
        #     '7DAY': ['30DAY', '90DAY', '180DAY', '360DAY'],
        #     '30DAY': ['90DAY', '180DAY', '360DAY'],
        #     '90DAY': ['180DAY', '360DAY'],
        #     '180DAY': ['360DAY']
        # }
        self.offset_day_mapping = {
            '30DAY': ['90DAY'],
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
        self.struct_range_string, self.struct_greater_string = Defaults.struct_range_string, \
                                                               Defaults.struct_greater_string
        self.list_of_operations = list(self.operation_filter) + self.two_level_operation + list(
            self.operation_count_filter)

        self.risk_indicator_lookback_days = [0, 30, 60, 90, 120, 150]

        ####

        self.alert_investigation_result = "ALERT_INVESTIGATION_RESULT"
        self.cdd_alert_investigation_result = "CDD_ALERT_INVESTIGATION_RESULT"

        self.vol_string = list(self.operation_vol_filter)[0]
        self.flow_through_adj_string = 'flow-through-adj'
        self.avg_operation = [x for x, y in self.operation_sum_filter.items()]

        self.monthly_avg_string = 'MONTHLY_AVG'
        self.avg_string = list(self.operation_avg_filter)[0]
        self.sum_string = list(self.operation_sum_filter)[0]

        self.txn_category_list = list(self.txn_category_filter)
        self.lookback_days_list = list(self.txn_lookback_filter)
        self.risk_country_string = list(self.risk_country_filter)[0]
        self.risk_party_string = list(self.risk_party_filter)[0]
        self.alert_lookback_days_list = list(self.alert_lookback_filter)
        self.alert_offset_days_list = list(self.alert_offset_filter)
        self.operation_filter_list = list(self.operation_filter)
        self.alert_operation_string = list(self.operation_count_filter)[0]
        self.offset_string = 'OFFSET'
        self.false_alert_string = list(self.false_alert_filter)[0]
        self.true_alert_string = list(self.true_alert_filter)[0]
        self.txngap_string = list(self.txngap_filter)[0]
        self.distinct_count_string = list(self.distinct_count_filter)[0]
        # assigning variables for the newly created columns inside the module for reusability
        self.txn_opp_party_key = "OPP_" + TRANSACTIONS.primary_party_key
        self.opp_country_code = "OPP_COUNTRY_CODE"
        self.txn_offset_days_list = list(self.txn_offset_filter)
        self.cdd_false_alert_string = list(self.cdd_false_alert_filter)[0]
        self.cdd_true_alert_string = list(self.cdd_true_alert_filter)[0]
        self.same_party_trf_string = list(self.same_party_trf_filter)[0]
        self.same_party_type_string = list(self.party_type_filter)[0]
        self.same_account_bu_string = list(self.account_bu_filter)[0]
        self.same_account_type_string = list(self.account_type_filter)[0]
        self.same_account_segment_string = list(self.account_segment_filter)[0]
        self.same_pep_string = list(self.pep_filter)[0]
        self.same_ngo_string = list(self.ngo_filter)[0]
        self.same_adverse_string = list(self.adverse_news_filter)[0]
        self.foreign_country_string = list(self.foreign_country_filter)[0]
        self.same_incorporate_tax_haven_string = list(self.incorporate_tax_haven_filter)[0]

        self.txn_monthly_mapping = {
            "3MONTH": "90DAY",
            "6MONTH": "180DAY",
            "12MONTH": "360DAY"
        }

        self.alert_features_identifier = [self.alert_string, list(self.false_alert_filter.keys())[0],
                                          list(self.true_alert_filter.keys())[0]]

        self.customer_activity_features_transTypes = list(set([i for sublist in
                                                               self.txn_category_filter.values()
                                                               for i in sublist])) + ["incoming-all", "outgoing-all"]

        self.operation_without_sd_filter = {"AMT": F.sum, "VOL": F.count, "MAX": F.max, "AVG": F.mean}

        self.txn_lookback_filter = {"DAILY": 1,
                                    "7DAY": 7,
                                    "30DAY": 30,
                                    "90DAY": 90,
                                    "180DAY": 180,
                                    "360DAY": 360}
        level = 'CUSTOMER'
        self.lookback_days_list = list(self.txn_lookback_filter)

        self.customer_activity_features_lookbackDays = ['7DAY', '30DAY', '90DAY', '180DAY', '360DAY']
        self.customer_activity_feature_operations = ['AMT', 'AVG', 'MAX', 'VOL']

        feature_formatter = lambda trans_type, lookback_day, operation_type, level: \
            trans_type + '_' + lookback_day + '_' + operation_type + '_' + level

        available_days = list(self.txn_lookback_filter.keys())

        self.customer_activity_features_lookbackDays = [d for d in self.customer_activity_features_lookbackDays if d
                                                        in available_days]

        self.customer_activity_features = []
        for trans_type in self.customer_activity_features_transTypes:
            for lookback_day in self.customer_activity_features_lookbackDays:
                for operation_type in self.customer_activity_feature_operations:
                    feature = feature_formatter(trans_type, lookback_day, operation_type, level)
                    self.customer_activity_features.append(feature)

        variety = 'TO-HIGH-RISK-COUNTRY'
        for trans_type in ['incoming-fund-transfer', 'outgoing-fund-transfer']:
            for lookback_day in self.customer_activity_features_lookbackDays:
                for operation_type in self.customer_activity_feature_operations:
                    f = trans_type + '_' + lookback_day + '_' + variety + '_' + operation_type + '_' + level
                    self.customer_activity_features.append(f)

        self.breakdown_first_level_features = []
        for trans_type in self.txn_category_filter.keys():
            for lookback_day in self.lookback_days_list:
                for operation_type in self.operation_without_sd_filter.keys():
                    for level in ['ACCOUNT', 'CUSTOMER']:
                        f = feature_formatter(trans_type, lookback_day, operation_type, level)
                        self.breakdown_first_level_features.append(f)

        self.ranges = [45000, 50000, 50000]
        self.young_age = 24

TRAIN_UNSUPERVISED_MODE = "train"
PREDICT_UNSUPERVISED_MODE = "predict"
PREDICT_SUPERVISED_MODE = "predict_supervised"
PREDICT_CDD_ALERTS_MODE = "cdd_alerts_predict"
VALID_UNSUPERVISED_MODE = "valid"
UNSUPERVISED_DATE_COL = 'CREATE_DATE'
ANOMALY_INDICATOR = 'FINAL_PREDICT'
UNION_BRANCH_CLUSTER_REMOVE_COLUMNS = ['_modelJobId_cluster']
NUM_PARTITIONS = 10 ** 3
RANDOM_NUM_RANGE = 10 ** 4
TXN_COUNT_THRESHOLD_SALT = 10 ** 5
SALT_REPARTION_NUMBER = 10 ** 4
INTERNAL_BROADCAST_LIMIT_MB = 700.0
UNSUPERVISED_MODEL_1_COLS = [
    CUSTOMERS.birth_incorporation_country,
    CUSTOMERS.customer_segment_code,
    CUSTOMERS.employment_status,
    "NUMBER_OF_ACCOUNTS",
    "high-risk-incoming_360DAY_AMT_CUSTOMER",
    "high-risk-outgoing_360DAY_AMT_CUSTOMER",
    "high-risk-outgoing_360DAY_MAX_CUSTOMER",
    "high-risk-outgoing_360DAY_VOL_CUSTOMER",
    "outgoing-all_12MONTH_MONTHLY_AVG_CUSTOMER"
]

STATIC_CUSTOMER_FEATURES = ConfigStaticFeatures().party_cols + [
    'LINKED_TRUEALERTS_COUNT',
    'NUMBER_OF_CUSTOMERS',
    'LINKED_HIGHRISK_COUNTRY_COUNT',
    'NUMBER_OF_ACCOUNTS',
    'LINKED_HIGHRISK_PARTY_COUNT',
    'LINKED_ALERTS_COUNT',
    'LINKED_TRUEALERT_CUSTOMER_COUNT',
    'PARTY_AGE',
    'HIGH_RISK_OCCUPATION',
    'HIGH_RISK_BUSINESS_TYPE',
    'NUMBER_OF_ACCOUNTS_OPENED_CLOSED_360DAYS',
    'NUMBER_OF_ACCOUNTS_OPENED_360DAYS'
]

BASE_COLS = [CDDALERTS.party_key, UNSUPERVISED_DATE_COL]
UNSUP_VALID_BASE_COLS = [CDDALERTS.alert_id, CDDALERTS.alert_created_date, CDDALERTS.party_key]

UNSUPERVISED_MODEL_2_COLS = [
    'cash-equivalent-deposit_90DAY_AMT_CUSTOMER',
    'cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER',
    'incoming-cheque_90DAY_AMT_CUSTOMER',
    'incoming-local-fund-transfer_90DAY_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
    'outgoing-cheque_90DAY_AMT_CUSTOMER',
    'outgoing-local-fund-transfer_90DAY_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
    'ATM-withdrawal_90DAY_AMT_CUSTOMER',
    'CDM-cash-deposit_90DAY_AMT_CUSTOMER',
    'card-charge_90DAY_AMT_CUSTOMER',
    'card-payment_90DAY_AMT_CUSTOMER']

STATIC_CLUSTER_FEATURES = ["CUSTOMER_SEGMENT_CODE",
                           "BIRTH_INCORPORATION_COUNTRY",
                           "INDIVIDUAL_CORPORATE_TYPE",
                           "BUSINESS_TYPE",
                           "OCCUPATION",
                           "EMPLOYMENT_STATUS",
                           "LINKED_HIGHRISK_COUNTRY_COUNT",
                           "NUMBER_OF_ACCOUNTS",
                           "LINKED_HIGHRISK_PARTY_COUNT",
                           "PARTY_AGE"]

TRANSACTIONS_VALUE_VOLUME_CLUSTER_FEATURES = [
    # 30 days
    'high-risk-incoming_30DAY_AMT_CUSTOMER',
    'high-risk-incoming_30DAY_VOL_CUSTOMER',
    'high-risk-outgoing_30DAY_AMT_CUSTOMER',
    'high-risk-outgoing_30DAY_MAX_CUSTOMER',
    'high-risk-outgoing_30DAY_VOL_CUSTOMER',
    'outgoing-all_12MONTH_MONTHLY_AVG_CUSTOMER',
    'incoming-all_12MONTH_MONTHLY_AVG_CUSTOMER',
    'cash-equivalent-deposit_30DAY_AMT_CUSTOMER',
    'cash-equivalent-deposit_30DAY_VOL_CUSTOMER',
    'cash-equivalent-withdrawal_30DAY_AMT_CUSTOMER',
    'cash-equivalent-withdrawal_30DAY_VOL_CUSTOMER',
    'CDM-cash-deposit_30DAY_AMT_CUSTOMER',
    'CDM-cash-deposit_30DAY_VOL_CUSTOMER',
    'outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_30DAY_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_30DAY_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_30DAY_VOL_CUSTOMER',
    # 90 days
    'high-risk-incoming_90DAY_AMT_CUSTOMER',
    'high-risk-incoming_90DAY_VOL_CUSTOMER',
    'high-risk-outgoing_90DAY_AMT_CUSTOMER',
    'high-risk-outgoing_90DAY_MAX_CUSTOMER',
    'high-risk-outgoing_90DAY_VOL_CUSTOMER',
    'outgoing-all_12MONTH_MONTHLY_AVG_CUSTOMER',
    'incoming-all_12MONTH_MONTHLY_AVG_CUSTOMER',
    'cash-equivalent-deposit_90DAY_AMT_CUSTOMER',
    'cash-equivalent-deposit_90DAY_VOL_CUSTOMER',
    'cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER',
    'cash-equivalent-withdrawal_90DAY_VOL_CUSTOMER',
    'CDM-cash-deposit_90DAY_AMT_CUSTOMER',
    'CDM-cash-deposit_90DAY_VOL_CUSTOMER',
    'outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_90DAY_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_90DAY_VOL_CUSTOMER',
    # 180 days
    'high-risk-incoming_180DAY_AMT_CUSTOMER',
    'high-risk-incoming_180DAY_VOL_CUSTOMER',
    'high-risk-outgoing_180DAY_AMT_CUSTOMER',
    'high-risk-outgoing_180DAY_MAX_CUSTOMER',
    'high-risk-outgoing_180DAY_VOL_CUSTOMER',
    'outgoing-all_12MONTH_MONTHLY_AVG_CUSTOMER',
    'incoming-all_12MONTH_MONTHLY_AVG_CUSTOMER',
    'cash-equivalent-deposit_180DAY_AMT_CUSTOMER',
    'cash-equivalent-deposit_180DAY_VOL_CUSTOMER',
    'cash-equivalent-withdrawal_180DAY_AMT_CUSTOMER',
    'cash-equivalent-withdrawal_180DAY_VOL_CUSTOMER',
    'CDM-cash-deposit_180DAY_AMT_CUSTOMER',
    'CDM-cash-deposit_180DAY_VOL_CUSTOMER',
    'outgoing-overseas-fund-transfer_180DAY_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_180DAY_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_180DAY_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_180DAY_VOL_CUSTOMER',
    # 360 days
    'high-risk-incoming_360DAY_AMT_CUSTOMER',
    'high-risk-incoming_360DAY_VOL_CUSTOMER',
    'high-risk-outgoing_360DAY_AMT_CUSTOMER',
    'high-risk-outgoing_360DAY_MAX_CUSTOMER',
    'high-risk-outgoing_360DAY_VOL_CUSTOMER',
    'outgoing-all_12MONTH_MONTHLY_AVG_CUSTOMER',
    'incoming-all_12MONTH_MONTHLY_AVG_CUSTOMER',
    'cash-equivalent-deposit_360DAY_AMT_CUSTOMER',
    'cash-equivalent-deposit_360DAY_VOL_CUSTOMER',
    'cash-equivalent-withdrawal_360DAY_AMT_CUSTOMER',
    'cash-equivalent-withdrawal_360DAY_VOL_CUSTOMER',
    'CDM-cash-deposit_360DAY_AMT_CUSTOMER',
    'CDM-cash-deposit_360DAY_VOL_CUSTOMER',
    'outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_360DAY_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_360DAY_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_360DAY_VOL_CUSTOMER', ]

TRANSACTIONS_COUNTER_PARTY_CLUSTER_FEATURES = [
    # 30 days
    'outgoing-fund-transfer_30DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER',
    'outgoing-fund-transfer_30DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER',
    'incoming-fund-transfer_30DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER',
    'incoming-fund-transfer_30DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER',
    'incoming-fund-transfer_30DAY_WITH-PEP_AMT_CUSTOMER',
    'incoming-fund-transfer_30DAY_WITH-PEP_VOL_CUSTOMER',
    'outgoing-fund-transfer_30DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-fund-transfer_30DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-all_30DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-all_30DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_30DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_30DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_30DAY_WITH-PEP_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_30DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-all_30DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'incoming-all_30DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'outgoing-all_30DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'outgoing-all_30DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'outgoing-overseas-fund-transfer_30DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_30DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_30DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_30DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    # 90 days
    'outgoing-fund-transfer_90DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER',
    'outgoing-fund-transfer_90DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER',
    'incoming-fund-transfer_90DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER',
    'incoming-fund-transfer_90DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER',
    'incoming-fund-transfer_90DAY_WITH-PEP_AMT_CUSTOMER',
    'incoming-fund-transfer_90DAY_WITH-PEP_VOL_CUSTOMER',
    'outgoing-fund-transfer_90DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-fund-transfer_90DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-all_90DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-all_90DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_90DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_90DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_90DAY_WITH-PEP_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_90DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-all_90DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'incoming-all_90DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'outgoing-all_90DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'outgoing-all_90DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'outgoing-overseas-fund-transfer_90DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_90DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_90DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_90DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    # 180 days
    'outgoing-fund-transfer_180DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER',
    'outgoing-fund-transfer_180DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER',
    'incoming-fund-transfer_180DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER',
    'incoming-fund-transfer_180DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER',
    'incoming-fund-transfer_180DAY_WITH-PEP_AMT_CUSTOMER',
    'incoming-fund-transfer_180DAY_WITH-PEP_VOL_CUSTOMER',
    'outgoing-fund-transfer_180DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-fund-transfer_180DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-all_180DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-all_180DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_180DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_180DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_180DAY_WITH-PEP_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_180DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-all_180DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'incoming-all_180DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'outgoing-all_180DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'outgoing-all_180DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'outgoing-overseas-fund-transfer_180DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_180DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_180DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_180DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    # 360 days
    'outgoing-fund-transfer_360DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER',
    'outgoing-fund-transfer_360DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER',
    'incoming-fund-transfer_360DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER',
    'incoming-fund-transfer_360DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER',
    'incoming-fund-transfer_360DAY_WITH-PEP_AMT_CUSTOMER',
    'incoming-fund-transfer_360DAY_WITH-PEP_VOL_CUSTOMER',
    'outgoing-fund-transfer_360DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-fund-transfer_360DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-all_360DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-all_360DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_360DAY_WITH-PEP_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_360DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_360DAY_WITH-PEP_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_360DAY_WITH-PEP_VOL_CUSTOMER',
    'incoming-all_360DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'incoming-all_360DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'outgoing-all_360DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'outgoing-all_360DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'outgoing-overseas-fund-transfer_360DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'outgoing-overseas-fund-transfer_360DAY_WITH-TAX-HAVEN_VOL_CUSTOMER',
    'incoming-overseas-fund-transfer_360DAY_WITH-TAX-HAVEN_AMT_CUSTOMER',
    'incoming-overseas-fund-transfer_360DAY_WITH-TAX-HAVEN_VOL_CUSTOMER', ]

TRANSACTIONS_HISTORICAL_CLUSTER_FEATURES = [
    # 30 days
    'net_incoming-all_30DAY_AMT_outgoing-all_30DAY_AMT_CUSTOMER',
    'net_incoming-overseas-fund-transfer_30DAY_AMT_outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER',
    'net_CDM-cash-deposit_30DAY_AMT_ATM-withdrawal_30DAY_AMT_CUSTOMER',
    'net_cash-equivalent-deposit_30DAY_AMT_cash-equivalent-withdrawal_30DAY_AMT_CUSTOMER',
    'ratio_CDM-cash-deposit_30DAY_AMT_ATM-withdrawal_30DAY_AMT_CUSTOMER',
    'ratio_cash-equivalent-deposit_30DAY_AMT_cash-equivalent-withdrawal_30DAY_AMT_CUSTOMER',
    'ratio_incoming-overseas-fund-transfer_30DAY_AMT_outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER',
    'ratio_incoming-all_30DAY_AMT_outgoing-all_30DAY_AMT_CUSTOMER',
    'flow-through-adj_cash-equivalent-withdrawal_30DAY_AMT_incoming-all_30DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-fund-transfer_30DAY_AMT_incoming-all_30DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-fund-transfer_30DAY_AMT_incoming-fund-transfer_30DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_30DAY_AMT_incoming-all_30DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_30DAY_AMT_cash-equivalent-deposit_30DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_30DAY_AMT_incoming-fund-transfer_30DAY_AMT_CUSTOMER',
    # 90 days
    'net_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER',
    'net_incoming-overseas-fund-transfer_90DAY_AMT_outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
    'net_CDM-cash-deposit_90DAY_AMT_ATM-withdrawal_90DAY_AMT_CUSTOMER',
    'net_cash-equivalent-deposit_90DAY_AMT_cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER',
    'ratio_CDM-cash-deposit_90DAY_AMT_ATM-withdrawal_90DAY_AMT_CUSTOMER',
    'ratio_cash-equivalent-deposit_90DAY_AMT_cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER',
    'ratio_incoming-overseas-fund-transfer_90DAY_AMT_outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
    'ratio_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER',
    'flow-through-adj_cash-equivalent-withdrawal_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-fund-transfer_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-fund-transfer_90DAY_AMT_incoming-fund-transfer_90DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_90DAY_AMT_cash-equivalent-deposit_90DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_90DAY_AMT_incoming-fund-transfer_90DAY_AMT_CUSTOMER',
    # 180 days
    'net_incoming-all_180DAY_AMT_outgoing-all_180DAY_AMT_CUSTOMER',
    'net_incoming-overseas-fund-transfer_180DAY_AMT_outgoing-overseas-fund-transfer_180DAY_AMT_CUSTOMER',
    'net_CDM-cash-deposit_180DAY_AMT_ATM-withdrawal_180DAY_AMT_CUSTOMER',
    'net_cash-equivalent-deposit_180DAY_AMT_cash-equivalent-withdrawal_180DAY_AMT_CUSTOMER',
    'ratio_CDM-cash-deposit_180DAY_AMT_ATM-withdrawal_180DAY_AMT_CUSTOMER',
    'ratio_cash-equivalent-deposit_180DAY_AMT_cash-equivalent-withdrawal_180DAY_AMT_CUSTOMER',
    'ratio_incoming-overseas-fund-transfer_180DAY_AMT_outgoing-overseas-fund-transfer_180DAY_AMT_CUSTOMER',
    'ratio_incoming-all_180DAY_AMT_outgoing-all_180DAY_AMT_CUSTOMER',
    'flow-through-adj_cash-equivalent-withdrawal_180DAY_AMT_incoming-all_180DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-fund-transfer_180DAY_AMT_incoming-all_180DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-fund-transfer_180DAY_AMT_incoming-fund-transfer_180DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_180DAY_AMT_incoming-all_180DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_180DAY_AMT_cash-equivalent-deposit_180DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_180DAY_AMT_incoming-fund-transfer_180DAY_AMT_CUSTOMER',
    # 360 days
    'net_incoming-all_360DAY_AMT_outgoing-all_360DAY_AMT_CUSTOMER',
    'net_incoming-overseas-fund-transfer_360DAY_AMT_outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER',
    'net_CDM-cash-deposit_360DAY_AMT_ATM-withdrawal_360DAY_AMT_CUSTOMER',
    'net_cash-equivalent-deposit_360DAY_AMT_cash-equivalent-withdrawal_360DAY_AMT_CUSTOMER',
    'ratio_CDM-cash-deposit_360DAY_AMT_ATM-withdrawal_360DAY_AMT_CUSTOMER',
    'ratio_cash-equivalent-deposit_360DAY_AMT_cash-equivalent-withdrawal_360DAY_AMT_CUSTOMER',
    'ratio_incoming-overseas-fund-transfer_360DAY_AMT_outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER',
    'ratio_incoming-all_360DAY_AMT_outgoing-all_360DAY_AMT_CUSTOMER',
    'flow-through-adj_cash-equivalent-withdrawal_360DAY_AMT_incoming-all_360DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-fund-transfer_360DAY_AMT_incoming-all_360DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-fund-transfer_360DAY_AMT_incoming-fund-transfer_360DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_360DAY_AMT_incoming-all_360DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_360DAY_AMT_cash-equivalent-deposit_360DAY_AMT_CUSTOMER',
    'flow-through-adj_outgoing-all_360DAY_AMT_incoming-fund-transfer_360DAY_AMT_CUSTOMER', ]

CLUSTER_CREATION_FEATURES_ALL = STATIC_CLUSTER_FEATURES + [feat for feat in TRANSACTIONS_VALUE_VOLUME_CLUSTER_FEATURES + \
                                                           TRANSACTIONS_COUNTER_PARTY_CLUSTER_FEATURES +
                                                           TRANSACTIONS_HISTORICAL_CLUSTER_FEATURES
                                                           if "360DAY" in feat]

TRANSACTIONS_VALUE_VOLUME_SCORING_FEATURES = ["cash-equivalent-deposit_30DAY_AMT_CUSTOMER",
                                              "cash-equivalent-deposit_30DAY_VOL_CUSTOMER",
                                              "cash-equivalent-withdrawal_30DAY_AMT_CUSTOMER",
                                              "cash-equivalent-withdrawal_30DAY_VOL_CUSTOMER",
                                              "incoming-overseas-fund-transfer_30DAY_AMT_CUSTOMER",
                                              "incoming-overseas-fund-transfer_30DAY_VOL_CUSTOMER",
                                              "outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER",
                                              "outgoing-overseas-fund-transfer_30DAY_VOL_CUSTOMER",
                                              "CDM-cash-deposit_30DAY_AMT_CUSTOMER",
                                              "CDM-cash-deposit_30DAY_VOL_CUSTOMER",
                                              "high-risk-incoming_30DAY_AMT_CUSTOMER",
                                              "high-risk-incoming_30DAY_VOL_CUSTOMER",
                                              "high-risk-outgoing_30DAY_AMT_CUSTOMER",
                                              "high-risk-outgoing_30DAY_MAX_CUSTOMER",
                                              "high-risk-outgoing_30DAY_VOL_CUSTOMER",
                                              "incoming-all_30DAY_AMT_CUSTOMER",
                                              "incoming-all_30DAY_VOL_CUSTOMER",
                                              "outgoing-all_30DAY_AMT_CUSTOMER",
                                              "outgoing-all_30DAY_VOL_CUSTOMER", ]

TRANSACTIONS_COUNTER_PARTY_SCORING_FEATURES = ["outgoing-fund-transfer_30DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER",
                                               "outgoing-fund-transfer_30DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER",
                                               "incoming-fund-transfer_30DAY_TO-HIGH-RISK-PARTY_AMT_CUSTOMER",
                                               "incoming-fund-transfer_30DAY_TO-HIGH-RISK-PARTY_VOL_CUSTOMER",
                                               "incoming-fund-transfer_30DAY_WITH-PEP_AMT_CUSTOMER",
                                               "incoming-fund-transfer_30DAY_WITH-PEP_VOL_CUSTOMER",
                                               "outgoing-fund-transfer_30DAY_WITH-PEP_AMT_CUSTOMER",
                                               "outgoing-fund-transfer_30DAY_WITH-PEP_VOL_CUSTOMER",
                                               "incoming-all_30DAY_WITH-PEP_AMT_CUSTOMER",
                                               "outgoing-all_30DAY_WITH-PEP_AMT_CUSTOMER",
                                               "outgoing-overseas-fund-transfer_30DAY_WITH-PEP_AMT_CUSTOMER",
                                               "outgoing-overseas-fund-transfer_30DAY_WITH-PEP_VOL_CUSTOMER",
                                               "incoming-overseas-fund-transfer_30DAY_WITH-PEP_AMT_CUSTOMER",
                                               "incoming-overseas-fund-transfer_30DAY_WITH-PEP_VOL_CUSTOMER",
                                               "incoming-all_30DAY_WITH-TAX-HAVEN_AMT_CUSTOMER",
                                               "incoming-all_30DAY_WITH-TAX-HAVEN_VOL_CUSTOMER",
                                               "outgoing-all_30DAY_WITH-TAX-HAVEN_AMT_CUSTOMER",
                                               "outgoing-all_30DAY_WITH-TAX-HAVEN_VOL_CUSTOMER",
                                               "outgoing-overseas-fund-transfer_30DAY_WITH-TAX-HAVEN_AMT_CUSTOMER",
                                               "outgoing-overseas-fund-transfer_30DAY_WITH-TAX-HAVEN_VOL_CUSTOMER",
                                               "incoming-overseas-fund-transfer_30DAY_WITH-TAX-HAVEN_AMT_CUSTOMER",
                                               "incoming-overseas-fund-transfer_30DAY_WITH-TAX-HAVEN_VOL_CUSTOMER", ]

TRANSACTIONS_HISTORICAL_SCORING_FEATURES = [
    "ratio_high-risk-incoming_30DAY_AMT_high-risk-incoming_90DAY_AMT_CUSTOMER",
    "ratio_high-risk-incoming_30DAY_VOL_high-risk-incoming_90DAY_VOL_CUSTOMER",
    "ratio_high-risk-outgoing_30DAY_AMT_high-risk-outgoing_90DAY_AMT_CUSTOMER",
    "ratio_high-risk-outgoing_30DAY_VOL_high-risk-outgoing_90DAY_VOL_CUSTOMER",
    "ratio_cash-equivalent-deposit_30DAY_AMT_cash-equivalent-deposit_90DAY_AMT_CUSTOMER",
    "ratio_cash-equivalent-deposit_30DAY_VOL_cash-equivalent-deposit_90DAY_VOL_CUSTOMER",
    "ratio_cash-equivalent-withdrawal_30DAY_AMT_cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER",
    "ratio_cash-equivalent-withdrawal_30DAY_VOL_cash-equivalent-withdrawal_90DAY_VOL_CUSTOMER",
    "ratio_incoming-overseas-fund-transfer_30DAY_AMT_incoming-overseas-fund-transfer_90DAY_AMT_CUSTOMER",
    "ratio_incoming-overseas-fund-transfer_30DAY_VOL_incoming-overseas-fund-transfer_90DAY_VOL_CUSTOMER",
    "ratio_outgoing-overseas-fund-transfer_30DAY_AMT_outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER",
    "ratio_outgoing-overseas-fund-transfer_30DAY_VOL_outgoing-overseas-fund-transfer_90DAY_VOL_CUSTOMER",
    "flow-through-adj_cash-equivalent-withdrawal_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER",
    "flow-through-adj_outgoing-all_90DAY_AMT_cash-equivalent-deposit_90DAY_AMT_CUSTOMER",
    "flow-through-adj_outgoing-fund-transfer_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER",
    "flow-through-adj_outgoing-all_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER", ]

CUSTOMER_SCORING_FEATURES_ALL = TRANSACTIONS_VALUE_VOLUME_SCORING_FEATURES + \
                                TRANSACTIONS_COUNTER_PARTY_SCORING_FEATURES + \
                                TRANSACTIONS_HISTORICAL_SCORING_FEATURES

ANOMALY_SCORING_COLS = ['CDM-cash-deposit_30DAY_AMT_CUSTOMER',
                        'outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER',
                        'high-risk-incoming_30DAY_AMT_CUSTOMER',
                        'high-risk-outgoing_30DAY_AMT_CUSTOMER',
                        'ratio_high-risk-incoming_30DAY_AMT_high-risk-incoming_90DAY_AMT_CUSTOMER',
                        'ratio_high-risk-outgoing_30DAY_AMT_high-risk-outgoing_90DAY_AMT_CUSTOMER',
                        'flow-through-adj_cash-equivalent-withdrawal_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
                        'flow-through-adj_outgoing-all_90DAY_AMT_cash-equivalent-deposit_90DAY_AMT_CUSTOMER',
                        'flow-through-adj_outgoing-fund-transfer_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
                        'flow-through-adj_outgoing-all_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER'
                        ]

UI_COLS = [
              ANOMALY_OUTPUT.party_key,
              ANOMALY_OUTPUT.party_age
          ] + ConfigCRSSemiSupervisedFeatureBox().customer_activity_features

# to override the pipeline data if the data is past data in POC
PIPELINE_OVERRIDE_TODAY_DATE = None
UNSUPERVISED_TARGET_COL = 'ALERT_INVESTIGATION_RESULT_CUSTOMER'

typology_map_df_schema = StructType([StructField("binary_filter", StringType()),
                                     StructField("extra_string", StringType()),
                                     StructField("feature_name", StringType()),
                                     StructField("level", StringType()),
                                     StructField("list_category", StringType()),
                                     StructField("lookback", StringType()),
                                     StructField("offset", StringType()),
                                     StructField("operated_column", StringType()),
                                     StructField("operation", StringType()),
                                     StructField("structured", StringType())])