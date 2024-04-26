try:
    from crs_utils import join_dicts
    from crs_prepipeline_tables \
        import CUSTOMERS, ACCOUNTS, TRANSACTIONS, TMALERTS, CDDALERTS, HIGHRISKCOUNTRY, HIGHRISKPARTY, C2A, C2C, CODES, \
        CURRENCYCONVERSIONRATE, CUSTOMERADDRESS, CUSTOMERCONTACT, FIGMAPPING, FIXEDDEPOSITS, LOANS, CARDS
    from crs_postpipeline_tables import UI_TMALERTS, UI_CDDALERTS, UI_ALERTEDCUSTOMERS, \
        UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS, UI_ALERTEDCUSTOMERSTOACCOUNTS, UI_TRANSACTIONS, CLUSTER_SCORE, MODEL_TABLE, \
        UI_UNIFIEDALERTS, CRS_CUSTOMERS
    from crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from crs_supervised_configurations import ConfigCRSSupervisedFeatureBox
    from constants import Defaults
except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables \
        import CUSTOMERS, ACCOUNTS, TRANSACTIONS, TMALERTS, CDDALERTS, HIGHRISKCOUNTRY, HIGHRISKPARTY, C2A, C2C, CODES, \
        CURRENCYCONVERSIONRATE, CUSTOMERADDRESS, CUSTOMERCONTACT, FIGMAPPING, FIXEDDEPOSITS, LOANS, CARDS
    from CustomerRiskScoring.src.crs_utils.crs_utils import join_dicts
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_TMALERTS, UI_CDDALERTS, UI_ALERTEDCUSTOMERS, \
    UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS, UI_ALERTEDCUSTOMERSTOACCOUNTS, UI_TRANSACTIONS, CLUSTER_SCORE, MODEL_TABLE, \
    UI_UNIFIEDALERTS, CRS_CUSTOMERS
    from CustomerRiskScoring.tables.crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from CustomerRiskScoring.config.crs_supervised_configurations import ConfigCRSSupervisedFeatureBox
    from Common.src.constants import Defaults


class ConfigCreateTMAlerts:
    """
    ConfigCreateTMAlerts: configuration for CreateTMAlerts
    """

    def __init__(self):
        # select required cols from ALERTS
        self.cols_from_input_alerts = [CDDALERTS.alert_id, CDDALERTS.alert_created_date, CDDALERTS.party_key,
                                       CDDALERTS.rule_name, CDDALERTS.account_key,
                                       CDDALERTS.bank_analyst_user_name, CDDALERTS.alert_closed_date,
                                       CDDALERTS.alert_investigation_result, CDDALERTS.tt_created_time]

        # select required cols from CUSTOMERS
        self.cols_from_party = [CUSTOMERS.party_key, CUSTOMERS.party_name, CUSTOMERS.customer_segment_code,
                                CUSTOMERS.customer_segment_name, CUSTOMERS.employee_flag]

        # select required cols from ANOMALY
        self.cols_from_anomaly = [ANOMALY_OUTPUT.anomaly_id, ANOMALY_OUTPUT.party_key, ANOMALY_OUTPUT.anomaly_date,
                                  ANOMALY_OUTPUT.cluster_id, ANOMALY_OUTPUT.anomaly_score, ANOMALY_OUTPUT.party_age,
                                  ANOMALY_OUTPUT.create_date]

        # select required cols from SUPERVISED
        self.cols_from_supervised = [CDD_SUPERVISED_OUTPUT.alert_id, CDD_SUPERVISED_OUTPUT.party_key,
                                     CDD_SUPERVISED_OUTPUT.account_key, CDD_SUPERVISED_OUTPUT.alert_created_date,
                                     CDD_SUPERVISED_OUTPUT.cluster_id, CDD_SUPERVISED_OUTPUT.prediction_score,
                                     CDD_SUPERVISED_OUTPUT.prediction_bucket, CDD_SUPERVISED_OUTPUT.party_age]

        # select required cols from ACCOUNTS
        self.cols_from_accounts = [ACCOUNTS.account_key, ACCOUNTS.type_code,
                                   ACCOUNTS.open_date, ACCOUNTS.currency_code]

        # select required cols from CLUSTER_SCORE
        self.cols_from_cluster_score = [CLUSTER_SCORE.cluster_id, CLUSTER_SCORE.cluster_score]

        # select required cols from MODEL_TABLE
        self.cols_from_model_table = [MODEL_TABLE.model_id]

        # after all necessary joins, rename to match postpipeline table output
        self.supervised_rename_mapping = {
            CDD_SUPERVISED_OUTPUT.alert_id: UI_CDDALERTS.alert_id,
            CDD_SUPERVISED_OUTPUT.alert_created_date: UI_CDDALERTS.alert_created_date,
            CDD_SUPERVISED_OUTPUT.party_key: UI_CDDALERTS.primary_party_key,
            CUSTOMERS.party_name: UI_CDDALERTS.primary_party_name,
            CDDALERTS.rule_name: UI_CDDALERTS.rules,
            CDD_SUPERVISED_OUTPUT.prediction_bucket: UI_CDDALERTS.alert_level,
            CDD_SUPERVISED_OUTPUT.prediction_score: UI_CDDALERTS.alert_score,
            CUSTOMERS.customer_segment_code: UI_CDDALERTS.segment_code,
            CUSTOMERS.customer_segment_name: UI_CDDALERTS.segment_name,
            CDD_SUPERVISED_OUTPUT.account_key: UI_CDDALERTS.account_key,
            ACCOUNTS.currency_code: UI_CDDALERTS.account_currency,
            ACCOUNTS.type_code: UI_CDDALERTS.account_type,
            ACCOUNTS.open_date: UI_CDDALERTS.account_open_date,
            CDD_SUPERVISED_OUTPUT.cluster_id: UI_CDDALERTS.cluster_id,
            CLUSTER_SCORE.cluster_score: UI_CDDALERTS.cluster_score,
            CDDALERTS.bank_analyst_user_name: UI_CDDALERTS.analyst_name,
            CDDALERTS.alert_closed_date: UI_CDDALERTS.closure_date,
            CDDALERTS.alert_investigation_result: UI_CDDALERTS.investigation_result,
            CUSTOMERS.employee_flag: UI_CDDALERTS.is_staff
        }

        # after all necessary joins, rename to match postpipeline table output
        self.anomaly_rename_mapping = {
            ANOMALY_OUTPUT.anomaly_id: UI_CDDALERTS.alert_id,
            ANOMALY_OUTPUT.anomaly_date: UI_CDDALERTS.alert_created_date,
            ANOMALY_OUTPUT.party_key: UI_CDDALERTS.primary_party_key,
            CUSTOMERS.party_name: UI_CDDALERTS.primary_party_name,
            ANOMALY_OUTPUT.anomaly_score: UI_CDDALERTS.alert_score,
            CUSTOMERS.customer_segment_code: UI_CDDALERTS.segment_code,
            CUSTOMERS.customer_segment_name: UI_CDDALERTS.segment_name,
            ANOMALY_OUTPUT.cluster_id: UI_CDDALERTS.cluster_id,
            CLUSTER_SCORE.cluster_score: UI_CDDALERTS.cluster_score,
            CUSTOMERS.employee_flag: UI_CDDALERTS.is_staff
        }

        # columns that are not available in any tables and need to be filled with below values
        self.supervised_fill_columns = {
            UI_CDDALERTS.alert_type: [0, 'int'],
            UI_CDDALERTS.hits_score: [0.0, 'double'],
            UI_CDDALERTS.created_user: [Defaults.CREATED_USER, Defaults.TYPE_STRING],
            UI_CDDALERTS.updated_timestamp: [None, Defaults.TYPE_TIMESTAMP],
            UI_CDDALERTS.updated_user: [None, Defaults.TYPE_STRING]
        }

        # columns that are not available in any tables and need to be filled with below values
        self.anomaly_fill_columns = {
            UI_CDDALERTS.alert_type: [1, 'int'],
            UI_CDDALERTS.account_key: [None, 'string'],
            UI_CDDALERTS.account_currency: [None, 'string'],
            UI_CDDALERTS.rules: [None, 'string'],
            UI_CDDALERTS.alert_level: [L3_STRING, 'string'],
            UI_CDDALERTS.account_type: [None, 'string'],
            UI_CDDALERTS.account_open_date: [None, 'timestamp'],
            UI_CDDALERTS.hits_score: [0.0, 'double'],
            UI_CDDALERTS.analyst_name: [None, 'string'],
            UI_CDDALERTS.closure_date: [None, 'timestamp'],
            UI_CDDALERTS.investigation_result: ['new', 'string'],
            UI_CDDALERTS.created_user: [Defaults.CREATED_USER, Defaults.TYPE_STRING],
            UI_CDDALERTS.updated_timestamp: [None, Defaults.TYPE_TIMESTAMP],
            UI_CDDALERTS.updated_user: [None, Defaults.TYPE_STRING]
        }


class ConfigCreateCDDAlerts:
    """
    ConfigCreateTMAlerts: configuration for CreateTMAlerts
    """

    def __init__(self):
        # select required cols from ALERTS
        self.cols_from_input_alerts = [CDDALERTS.alert_id, CDDALERTS.alert_created_date, CDDALERTS.party_key,
                                       CDDALERTS.rule_name, CDDALERTS.bank_analyst_user_name,
                                       CDDALERTS.alert_closed_date,
                                       CDDALERTS.alert_investigation_result, CDDALERTS.tt_created_time]

        # select required cols from CUSTOMERS
        self.cols_from_party = [CUSTOMERS.party_key, CUSTOMERS.party_name, CUSTOMERS.customer_segment_code,
                                CUSTOMERS.customer_segment_name, CUSTOMERS.employee_flag]

        # select required cols from ANOMALY
        self.cols_from_anomaly = [ANOMALY_OUTPUT.anomaly_id, ANOMALY_OUTPUT.party_key, ANOMALY_OUTPUT.anomaly_date,
                                  ANOMALY_OUTPUT.cluster_id, ANOMALY_OUTPUT.anomaly_score, ANOMALY_OUTPUT.party_age,
                                  ANOMALY_OUTPUT.create_date]

        # select required cols from SUPERVISED
        self.cols_from_supervised = [CDD_SUPERVISED_OUTPUT.alert_id, CDD_SUPERVISED_OUTPUT.party_key,
                                     CDD_SUPERVISED_OUTPUT.alert_created_date,
                                     CDD_SUPERVISED_OUTPUT.cluster_id, CDD_SUPERVISED_OUTPUT.prediction_score,
                                     CDD_SUPERVISED_OUTPUT.prediction_bucket, CDD_SUPERVISED_OUTPUT.party_age]

        self.cols_from_ensemble = [CDD_SUPERVISED_OUTPUT.alert_id, CDD_SUPERVISED_OUTPUT.party_key,
                                   CDD_SUPERVISED_OUTPUT.alert_created_date,
                                   CDD_SUPERVISED_OUTPUT.cluster_id, ANOMALY_OUTPUT.ensemble_score,
                                   CDD_SUPERVISED_OUTPUT.risk_bucket, CDD_SUPERVISED_OUTPUT.party_age]

        # select required cols from ACCOUNTS
        self.cols_from_accounts = [ACCOUNTS.account_key, ACCOUNTS.type_code,
                                   ACCOUNTS.open_date, ACCOUNTS.currency_code]

        # select required cols from CLUSTER_SCORE
        self.cols_from_cluster_score = [CLUSTER_SCORE.cluster_id, CLUSTER_SCORE.cluster_score]

        # select required cols from MODEL_TABLE
        self.cols_from_model_table = [MODEL_TABLE.model_id]

        # after all necessary joins, rename to match postpipeline table output
        self.supervised_rename_mapping = {
            CDD_SUPERVISED_OUTPUT.alert_id: UI_CDDALERTS.alert_id,
            CDD_SUPERVISED_OUTPUT.alert_created_date: UI_CDDALERTS.alert_created_date,
            CDD_SUPERVISED_OUTPUT.party_key: UI_CDDALERTS.primary_party_key,
            CUSTOMERS.party_name: UI_CDDALERTS.primary_party_name,
            CDDALERTS.rule_name: UI_CDDALERTS.rules,
            CDD_SUPERVISED_OUTPUT.risk_bucket: UI_CDDALERTS.alert_level,
            ANOMALY_OUTPUT.ensemble_score: UI_CDDALERTS.alert_score,
            CUSTOMERS.customer_segment_code: UI_CDDALERTS.segment_code,
            CUSTOMERS.customer_segment_name: UI_CDDALERTS.segment_name,
            TMALERTS.account_key: UI_TMALERTS.account_key,
            ACCOUNTS.currency_code: UI_CDDALERTS.account_currency,
            ACCOUNTS.type_code: UI_CDDALERTS.account_type,
            ACCOUNTS.open_date: UI_CDDALERTS.account_open_date,
            CDD_SUPERVISED_OUTPUT.cluster_id: UI_CDDALERTS.cluster_id,
            CLUSTER_SCORE.cluster_score: UI_CDDALERTS.cluster_score,
            CDDALERTS.bank_analyst_user_name: UI_CDDALERTS.analyst_name,
            CDDALERTS.alert_closed_date: UI_CDDALERTS.closure_date,
            CDDALERTS.alert_investigation_result: UI_CDDALERTS.investigation_result,
            CUSTOMERS.employee_flag: UI_CDDALERTS.is_staff
        }

        # after all necessary joins, rename to match postpipeline table output
        self.anomaly_rename_mapping = {
            ANOMALY_OUTPUT.anomaly_id: UI_CDDALERTS.alert_id,
            ANOMALY_OUTPUT.anomaly_date: UI_CDDALERTS.alert_created_date,
            ANOMALY_OUTPUT.party_key: UI_CDDALERTS.primary_party_key,
            CUSTOMERS.party_name: UI_CDDALERTS.primary_party_name,
            ANOMALY_OUTPUT.anomaly_score: UI_CDDALERTS.alert_score,
            CUSTOMERS.customer_segment_code: UI_CDDALERTS.segment_code,
            CUSTOMERS.customer_segment_name: UI_CDDALERTS.segment_name,
            ANOMALY_OUTPUT.cluster_id: UI_CDDALERTS.cluster_id,
            CLUSTER_SCORE.cluster_score: UI_CDDALERTS.cluster_score,
            CUSTOMERS.employee_flag: UI_CDDALERTS.is_staff
        }

        # columns that are not available in any tables and need to be filled with below values
        self.supervised_fill_columns = {
            UI_CDDALERTS.alert_type: [2, 'int'],
            UI_CDDALERTS.hits_score: [0.0, 'double'],
            UI_CDDALERTS.created_user: [Defaults.CREATED_USER, Defaults.TYPE_STRING],
            UI_CDDALERTS.updated_timestamp: [None, Defaults.TYPE_TIMESTAMP],
            UI_CDDALERTS.updated_user: [None, Defaults.TYPE_STRING]
        }

        # columns that are not available in any tables and need to be filled with below values
        self.anomaly_fill_columns = {
            UI_CDDALERTS.alert_type: [1, 'int'],
            UI_TMALERTS.account_key: [None, 'string'],
            UI_CDDALERTS.account_currency: [None, 'string'],
            UI_CDDALERTS.rules: [None, 'string'],
            UI_CDDALERTS.alert_level: [L3_STRING, 'string'],
            UI_CDDALERTS.account_type: [None, 'string'],
            UI_CDDALERTS.account_open_date: [None, 'timestamp'],
            UI_CDDALERTS.hits_score: [0.0, 'double'],
            UI_CDDALERTS.analyst_name: [None, 'string'],
            UI_CDDALERTS.closure_date: [None, 'timestamp'],
            UI_CDDALERTS.investigation_result: ['new', 'string'],
            UI_CDDALERTS.created_user: [Defaults.CREATED_USER, Defaults.TYPE_STRING],
            UI_CDDALERTS.updated_timestamp: [None, Defaults.TYPE_TIMESTAMP],
            UI_CDDALERTS.updated_user: [None, Defaults.TYPE_STRING]
        }
        self.alert_status_dict = {'0': ['NEW', 'REOPEN', 'New'],
                                  '1': ["Work in Progress", "UNDER_INVESTIGATION", "WORK IN PROGRESS",
                                        "PENDING CLOSURE", "Linked", "Pending Closure", "Pending Case Creation",
                                        "Pending Exclusion", "Referred to GC", "Work In Progress"],
                                  '2': ['CLOSED', 'LINKED ALERT-CLOSED', 'ALERT-CLOSED', "Alert-Closed", "Closed",
                                        "Excluded - Closed", "Linked Alert - Closed", "Alert-Closed", "Excluded-Closed",
                                        "Linked Alert- Closed"]}
        self.label_dict = {"True Hit": ['True Hit :PEP/RCA with no adverse news', 'True Hit :Relationship allowed nMAN',
                                        'True Hit : non PEP/RCA - Relationship allowed nMAN',
                                        'True Hit : non PEP/RCA - Relationship allowed MAN',
                                        'True Hit :Relationship allowed MAN',
                                        'True Hit:  Non PEP/RCA - No existing relationship with customer',
                                        'True Hit:  Non PEP/RCA - Exit / disallowed relationship MAN',
                                        'True Hit : Non PEP/RCA - No existing relationship with customer',
                                        'True Hit : Non PEP/RCA - Exit /disallowed relationship MAN',
                                        'True Hit : Continuation allowed (material adverse news)',
                                        'True Hit : Continuation disallowed (material adverse news)',
                                        'True Hit : Continuation allowed (non-material adverse news)',
                                        'True Hit:  Non PEP/RCA - Relationship allowed MAN',
                                        'True Hit: PEP/RCA - Relationship allowed MAN',
                                        'True Hit: PEP/RCA - Exit / disallowed MAN',
                                        'True Hit:  PEP/RCA - No existing relationship with customer',
                                        'True Hit: Continuation allowed (material adverse news)',
                                        'True Hit: Continuation disallowed (material adverse news)',
                                        'True Hit: Further investigation required',
                                        'True Hit: Non PEP/RCA - Relationship allowed nMAN',
                                        'True Hit: PEP/RCA - Exit / disallowed relationship MAN', 'Customer is a pep',
                                        'Customer is a PEP', 'True Hit: Continuation allowed (material adverse news)',
                                        'True Hit: PEP/RCA - Relationship allowed nMAN',
                                        'True Hit: PEP/RCA with no adverse news',
                                        'True Hit – Relationship allowed (Indirect Sanctions Exposure)',
                                        'True Hit – Relationship allowed (Shell / Front Company)',
                                        'True Hit – Relationship allowed (Tax Concerns/Tax Evasion)',
                                        'True Hit – Relationship allowed (TBML/Trade Fraud)',
                                        'True Hit – Relationship allowed (Material Adverse News)',
                                        'True Hit – Relationship allowed (Non Material Adverse News)',
                                        'True Hit - no adverse news',
                                        'True Hit – PEP/RCA – Relationship allowed (Indirect Sanctions Exposure)',
                                        'True Hit – PEP/RCA - Relationship allowed (Tax Concerns/Tax Evasion)',
                                        'True Hit – PEP/RCA - Relationship allowed (TBML/Trade Fraud)',
                                        'True Hit – PEP/RCA – Relationship allowed (Material Adverse News)',
                                        'True Hit – PEP/RCA – Relationship allowed (Non Material Adverse News)',
                                        'True Hit – PEP/RCA with no adverse news',
                                        'True Hit - Exit / Disallowed relationship (Material Adverse News)',
                                        'True Hit - Exit / Disallowed relationship (Direct Sanctions Exposure)',
                                        'True Hit - Exit / Disallowed relationship (Shell / Front Company)',
                                        'True Hit - Exit / Disallowed relationship (Tax crimes /Tax evasion)',
                                        'True Hit - Exit / Disallowed relationship (Correspondent Banking Risk)',
                                        'True Hit - Exit / Disallowed relationship (TBML/Trade Fraud)'],
                           "False Hit": ['False Hit : one or more Secondary identifiers differ',
                                         'False Hit: primary identifier differs (to specify in comments)',
                                         'False Hit :Primary and Secondary identifiers differ',
                                         'False Hit : to send for exclusion',
                                         'False Hit: One or more Secondary identifier differ',
                                         'False Hit:  Primary and secondary identifiers differ',
                                         'False Hit: One or more Secondary identifiers differ',
                                         'False Hit: Primary identifier differs (to specify in comments)',
                                         'False Hit - Primary and/or Secondary identifier differs'],
                           "Insufficient Hit": ['Insufficient Identifiers: Relationship allowed MAN',
                                                'Insufficient Idnetifiers : Relationship allowed nMAN',
                                                'Insufficient Identifiers: No existing relationship with customer',
                                                'Insufficient Identifiers: Exit /Disallow relationship MAN',
                                                'Insufficient Identifiers', 'More information required',
                                                'Further Investigation Required',
                                                'Insufficient Identifiers: Exit / Disallow relationship MAN',
                                                'Insufficient Identifiers: Relationship allowed nMAN',
                                                'Reopen: Further investigation required',
                                                'No existing banking relationship',
                                                'Insufficient identifiers - Exit / Disallowed relationship (Material '
                                                'Adverse News)',
                                                'Insufficient identifiers - Relationship allowed']}


class ConfigCreateAlertedCustomers:
    """
    ConfigCreateAlertedCustomers: configuration for CreateAlertedCustomers
    """

    def __init__(self):
        self.cols_from_customers = [CUSTOMERS.party_key, CUSTOMERS.party_name, CUSTOMERS.individual_corporate_type,
                                    CUSTOMERS.risk_level, CUSTOMERS.occupation, CUSTOMERS.gender,
                                    CUSTOMERS.customer_segment_code, CUSTOMERS.customer_segment_name,
                                    CUSTOMERS.date_of_birth_or_incorporation, CUSTOMERS.customer_division,
                                    CUSTOMERS.domicile_country, CUSTOMERS.residential_address, CUSTOMERS.postal_code,
                                    CUSTOMERS.employee_flag, CUSTOMERS.pep_flag, CUSTOMERS.customer_contact_no,
                                    CUSTOMERS.customer_id_no, CUSTOMERS.customer_id_type,
                                    CUSTOMERS.customer_id_country, CUSTOMERS.employer_name,
                                    CUSTOMERS.annual_turnover, CUSTOMERS.designation, CUSTOMERS.nature_of_business,
                                    CUSTOMERS.residence_operation_country, CUSTOMERS.citizenship_country,
                                    CUSTOMERS.birth_incorporation_country,
                                    CUSTOMERS.acquisition_date, CUSTOMERS.status_code, CUSTOMERS.business_profession]

        self.cols_from_high_risk_party = [HIGHRISKPARTY.party_key, HIGHRISKPARTY.high_risk_start_date,
                                          HIGHRISKPARTY.high_risk_expiry_date, HIGHRISKPARTY.high_risk_reason]

        self.cols_from_supervised = [CDD_SUPERVISED_OUTPUT.alert_id, CDD_SUPERVISED_OUTPUT.alert_created_date,
                                     CDD_SUPERVISED_OUTPUT.party_key, CDD_SUPERVISED_OUTPUT.party_age]

        self.cols_from_anomaly = [ANOMALY_OUTPUT.anomaly_id, ANOMALY_OUTPUT.anomaly_date, ANOMALY_OUTPUT.party_key,
                                  ANOMALY_OUTPUT.party_age]

        # after all necessary joins, rename to match postpipeline table output
        self.rename_mapping = {
            CDD_SUPERVISED_OUTPUT.alert_id: UI_ALERTEDCUSTOMERS.alert_id,
            CDD_SUPERVISED_OUTPUT.party_key: UI_ALERTEDCUSTOMERS.party_key,
            CUSTOMERS.risk_level: UI_ALERTEDCUSTOMERS.risk_rating,
            HIGHRISKPARTY.high_risk_additional_info: UI_ALERTEDCUSTOMERS.high_risk_party_info,
            CDD_SUPERVISED_OUTPUT.party_age: UI_ALERTEDCUSTOMERS.age,
            CUSTOMERS.occupation: UI_ALERTEDCUSTOMERS.occupation,
            CUSTOMERS.customer_segment_code: UI_ALERTEDCUSTOMERS.segment_code,
            CUSTOMERS.customer_segment_name: UI_ALERTEDCUSTOMERS.segment,
            CUSTOMERS.domicile_country: UI_ALERTEDCUSTOMERS.domicile_country,
            CUSTOMERS.party_key: CRS_CUSTOMERS.customer_key,
            CUSTOMERS.party_name: CRS_CUSTOMERS.customer_name,
            CUSTOMERS.date_of_birth_or_incorporation: CRS_CUSTOMERS.date_of_birth_incorporation,
            CUSTOMERS.acquisition_date: CRS_CUSTOMERS.customer_onboard_date,
            CUSTOMERS.individual_corporate_type: CRS_CUSTOMERS.customer_type,
            CUSTOMERS.business_profession: CRS_CUSTOMERS.customer_industry,
            CUSTOMERS.status_code: CRS_CUSTOMERS.customer_status,
            CUSTOMERS.customer_division: CRS_CUSTOMERS.division,
            CUSTOMERS.nature_of_business: CRS_CUSTOMERS.type_of_business,
            CUSTOMERS.residential_address: CRS_CUSTOMERS.residential_address,
            CUSTOMERS.postal_code: CRS_CUSTOMERS.postal_code,
            CUSTOMERS.customer_contact_no: CRS_CUSTOMERS.contact_number,
            CUSTOMERS.designation: CRS_CUSTOMERS.designation,
            CUSTOMERS.gender: CRS_CUSTOMERS.gender,
            CUSTOMERS.citizenship_country: CRS_CUSTOMERS.nationality,
            CUSTOMERS.customer_id_no: CRS_CUSTOMERS.id_number,
            CUSTOMERS.customer_id_type: CRS_CUSTOMERS.id_type,
            CUSTOMERS.customer_id_country: CRS_CUSTOMERS.id_country,
            CUSTOMERS.employer_name: CRS_CUSTOMERS.employer_name,
            CUSTOMERS.pep_flag: CRS_CUSTOMERS.pep_ind,
            CUSTOMERS.employee_flag: CRS_CUSTOMERS.bank_staff_indicator,
            CUSTOMERS.annual_turnover: CRS_CUSTOMERS.annual_turnover,
            CUSTOMERS.birth_incorporation_country: CRS_CUSTOMERS.birth_incorporation_country,
            CUSTOMERS.residence_operation_country: CRS_CUSTOMERS.residence_operation_country
        }
        # columns that are not available in any tables and need to be filled with below values
        self.fill_columns = {
            CRS_CUSTOMERS.kyc_review_date: [None, 'timestamp'],
        }

        # FIXME: HARD FIX as UI HAS A HARD REQUIREMENT OF CUSTOMER_TYPE TO BE EITHER "I" OR "C"
        #  THIS WILL BE FIXED IN ETL IN THE FUTURE RELEASES [TEMP WORKAROUND]

        self.customer_type_mapping = {
            "I": ["P", "I", "Individual", "INDIVIDUAL"],
            "C": ["C", "Corporate", "CORPORATE"]
        }

        self.individual_customer_type_string = "I"

        self.customer_activity_features = ConfigCRSSupervisedFeatureBox().customer_activity_features

        txn_category_filter = ConfigCRSSupervisedFeatureBox().txn_category_filter
        self.CUSTOMER_ACTIVITY_COL = "ACTIVITY_DETAILS_TXN_BY_TYPE"
        self.CUSTOMER_ACTIVITY_ADDITIONAL_INFO_COL = "ACTIVITY_DETAILS_TXN_ADDITIONAL_INFO"

        self.customer_activity_additonal_info_dict = {"IN": {"CASH": list(txn_category_filter["CDM-cash-deposit"] +
                                                                          txn_category_filter[
                                                                              "cash-equivalent-deposit"]),
                                                             "NON-CASH": list(txn_category_filter["card-payment"] +
                                                                              txn_category_filter["incoming-cheque"] +
                                                                              txn_category_filter[
                                                                                  "incoming-local-fund-transfer"] +
                                                                              txn_category_filter[
                                                                                  "Misc-credit"] + txn_category_filter[
                                                                                  "cash-equivalent-card-payment"]
                                                                              ),
                                                             "REMITTANCE": list(txn_category_filter[
                                                                                    "incoming-overseas-fund-transfer"])},
                                                      "OUT": {"CASH": list(txn_category_filter["ATM-withdrawal"] +
                                                                           txn_category_filter[
                                                                               "cash-equivalent-withdrawal"]),
                                                              "NON-CASH": list(txn_category_filter["card-charge"] +
                                                                               txn_category_filter["outgoing-cheque"] +
                                                                               txn_category_filter[
                                                                                   "outgoing-local-fund-transfer"] +
                                                                               txn_category_filter[
                                                                                   "Misc-debit"]),
                                                              "REMITTANCE": list(txn_category_filter[
                                                                                     "outgoing-overseas-fund-transfer"])}

                                                      }


class ConfigCreateAlertedCustomersLinkedCustomers:
    """
    ConfigCreateAlertedCustomersLinkedCustomers: configuration for CreateAlertedCustomersLinkedCustomers:
    """

    def __init__(self):
        self.cols_from_customers = [CUSTOMERS.party_key, CUSTOMERS.party_name, CUSTOMERS.individual_corporate_type,
                                    CUSTOMERS.nature_of_business, CUSTOMERS.risk_level, CUSTOMERS.occupation,
                                    CUSTOMERS.customer_segment_code, CUSTOMERS.customer_segment_name,
                                    CUSTOMERS.citizenship_country, CUSTOMERS.date_of_birth_or_incorporation,
                                    CUSTOMERS.employee_flag]

        self.cols_from_c2c = [C2C.party_key, C2C.linked_party_key, C2C.relation_code,
                              C2C.relationship_description, C2C.relationship_end_date, C2C.shareholder_percentage]

        self.cols_from_supervised = [CDD_SUPERVISED_OUTPUT.alert_id, CDD_SUPERVISED_OUTPUT.party_key,
                                     CDD_SUPERVISED_OUTPUT.alert_created_date]

        self.cols_from_anomaly = [ANOMALY_OUTPUT.party_key, ANOMALY_OUTPUT.create_date]

        # after all necessary joins, rename to match postpipeline table output
        self.rename_mapping = {
            CDD_SUPERVISED_OUTPUT.alert_id: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.alert_id,
            CDD_SUPERVISED_OUTPUT.party_key: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.primary_party_key,
            C2C.linked_party_key: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.linked_party_key,
            CUSTOMERS.party_name: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.party_name,
            CUSTOMERS.risk_level: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.risk_rating,
            CUSTOMERS.customer_segment_code: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.segment_code,
            CUSTOMERS.customer_segment_name: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.segment,
            CUSTOMERS.citizenship_country: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.nationality,
            CUSTOMERS.date_of_birth_or_incorporation: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.dob_or_incorporation_date,
            C2C.relation_code: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.relation_type,
            C2C.relationship_description: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.relation_description,
            C2C.shareholder_percentage: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.shareholder_percentage,
            CUSTOMERS.employee_flag: UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.is_staff
        }

        self.customer_type_mapping = {
            "I": ["P", "I", "Individual", "INDIVIDUAL"],
            "C": ["C", "Corporate", "CORPORATE"]
        }

        self.individual_customer_type_string = "I"


class ConfigCreateAlertedCustomerstoAccounts:
    """
    ConfigCreateAlertedCustomerstoAccounts: configuration for CreateAlertedCustomerstoAccounts
    """

    def __init__(self):
        self.cols_from_accounts = [ACCOUNTS.account_key, ACCOUNTS.account_number, ACCOUNTS.currency_code,
                                   ACCOUNTS.type_code, ACCOUNTS.account_name, ACCOUNTS.open_date, ACCOUNTS.status_code,
                                   ACCOUNTS.closed_date, ACCOUNTS.opening_balance, ACCOUNTS.avg_debit_balance,
                                   ACCOUNTS.avg_credit_balance]
        self.cols_from_c2a = [C2A.party_key, C2A.account_key, C2A.relation_code]
        self.cols_from_supervised = [CDD_SUPERVISED_OUTPUT.alert_id,
                                     CDD_SUPERVISED_OUTPUT.party_key]
        self.cols_from_anomaly = [ANOMALY_OUTPUT.party_key,
                                  ANOMALY_OUTPUT.create_date]
        self.cols_from_loans = [LOANS.account_key, LOANS.outstanding_loan_balance, LOANS.loan_amount,
                                LOANS.installment_value, LOANS.loan_start_date, LOANS.anticipated_settlement_date,
                                LOANS.total_repayment_amount, LOANS.number_of_payments, LOANS.gl_group,
                                LOANS.loan_purpose_code, LOANS.loan_type_description, LOANS.loan_key]
        self.cols_from_cards = [CARDS.account_key, CARDS.card_number, CARDS.card_key, CARDS.acct_curr_credit_limit]
        self.cols_from_fixed_deposits = [FIXEDDEPOSITS.fd_key, FIXEDDEPOSITS.account_key, FIXEDDEPOSITS.fd_status_code,
                                         FIXEDDEPOSITS.fd_receipt_number, FIXEDDEPOSITS.fd_original_amount,
                                         FIXEDDEPOSITS.fd_date, FIXEDDEPOSITS.fd_maturity_date,
                                         FIXEDDEPOSITS.fd_withdrawal_date]

        # after all necessary joins, rename to match postpipeline table output
        self.rename_mapping = {
            CDD_SUPERVISED_OUTPUT.alert_id: UI_ALERTEDCUSTOMERSTOACCOUNTS.alert_id,
            ACCOUNTS.account_number: UI_ALERTEDCUSTOMERSTOACCOUNTS.account_id,
            ACCOUNTS.account_key: UI_ALERTEDCUSTOMERSTOACCOUNTS.account_key,
            C2A.relation_code: UI_ALERTEDCUSTOMERSTOACCOUNTS.c2a_relationship,
            ACCOUNTS.type_code: UI_ALERTEDCUSTOMERSTOACCOUNTS.product,
            ACCOUNTS.open_date: UI_ALERTEDCUSTOMERSTOACCOUNTS.open_date,
            ACCOUNTS.opening_balance: UI_ALERTEDCUSTOMERSTOACCOUNTS.balance,
            CARDS.acct_curr_credit_limit: UI_ALERTEDCUSTOMERSTOACCOUNTS.credit_limit,
            CARDS.card_number: UI_ALERTEDCUSTOMERSTOACCOUNTS.card_number,
            ACCOUNTS.account_name: UI_ALERTEDCUSTOMERSTOACCOUNTS.account_name,
            ACCOUNTS.closed_date: UI_ALERTEDCUSTOMERSTOACCOUNTS.account_closed_date,
            ACCOUNTS.status_code: UI_ALERTEDCUSTOMERSTOACCOUNTS.account_status,
            ACCOUNTS.currency_code: UI_ALERTEDCUSTOMERSTOACCOUNTS.account_currency,
            ACCOUNTS.avg_debit_balance: UI_ALERTEDCUSTOMERSTOACCOUNTS.account_dr_avg_balance,
            ACCOUNTS.avg_credit_balance: UI_ALERTEDCUSTOMERSTOACCOUNTS.account_cr_avg_balance,
            FIXEDDEPOSITS.fd_receipt_number: UI_ALERTEDCUSTOMERSTOACCOUNTS.fd_receipt_number,
            FIXEDDEPOSITS.fd_original_amount: UI_ALERTEDCUSTOMERSTOACCOUNTS.fd_original_amount,
            FIXEDDEPOSITS.fd_date: UI_ALERTEDCUSTOMERSTOACCOUNTS.fd_placement_date,
            FIXEDDEPOSITS.fd_maturity_date: UI_ALERTEDCUSTOMERSTOACCOUNTS.fd_maturity_date,
            FIXEDDEPOSITS.fd_withdrawal_date: UI_ALERTEDCUSTOMERSTOACCOUNTS.fd_withdrawal_date,
            LOANS.outstanding_loan_balance: UI_ALERTEDCUSTOMERSTOACCOUNTS.outstanding_loan_balance,
            LOANS.loan_amount: UI_ALERTEDCUSTOMERSTOACCOUNTS.loan_amount,
            LOANS.installment_value: UI_ALERTEDCUSTOMERSTOACCOUNTS.loan_installment_amount,
            LOANS.loan_type_description: UI_ALERTEDCUSTOMERSTOACCOUNTS.loan_type,
            LOANS.loan_purpose_code: UI_ALERTEDCUSTOMERSTOACCOUNTS.loan_purpose_code,
            LOANS.gl_group: UI_ALERTEDCUSTOMERSTOACCOUNTS.loan_gl_group,
            LOANS.loan_start_date: UI_ALERTEDCUSTOMERSTOACCOUNTS.loan_disbursement_date,
            LOANS.anticipated_settlement_date: UI_ALERTEDCUSTOMERSTOACCOUNTS.loan_maturity_date,
            LOANS.total_repayment_amount: UI_ALERTEDCUSTOMERSTOACCOUNTS.total_repaid_amount,
            LOANS.number_of_payments: UI_ALERTEDCUSTOMERSTOACCOUNTS.number_of_repayments
        }

        # columns that are not available in any tables and need to be filled with below values
        self.derive_columns = {
            UI_ALERTEDCUSTOMERSTOACCOUNTS.loan_group: LOANS.gl_group,
        }

        # columns that are not available in any tables and need to be filled with below values
        self.fill_columns = {
            UI_ALERTEDCUSTOMERSTOACCOUNTS.created_user: [Defaults.CREATED_USER, Defaults.TYPE_STRING],
            UI_ALERTEDCUSTOMERSTOACCOUNTS.updated_timestamp: [None, Defaults.TYPE_TIMESTAMP],
            UI_ALERTEDCUSTOMERSTOACCOUNTS.updated_user: [None, Defaults.TYPE_STRING]
        }


class ConfigCreateUITransactions:
    """
    ConfigCreateUITransactions: configuration for CreateUITransactions
    """

    def __init__(self):
        self.cols_from_supervised = [CDD_SUPERVISED_OUTPUT.party_key]
        self.cols_from_anomaly = [ANOMALY_OUTPUT.party_key]
        self.cols_from_accounts = [ACCOUNTS.account_key, ACCOUNTS.account_number]
        self.cols_from_fd = [FIXEDDEPOSITS.account_key, FIXEDDEPOSITS.fd_td_number]

        self.cols_from_txns = [TRANSACTIONS.transaction_key, TRANSACTIONS.primary_party_key, TRANSACTIONS.txn_date_time,
                               TRANSACTIONS.product_code, TRANSACTIONS.acct_currency_code,
                               TRANSACTIONS.acct_currency_amount, TRANSACTIONS.orig_currency_code,
                               TRANSACTIONS.orig_currency_amount, TRANSACTIONS.txn_amount, TRANSACTIONS.account_key,
                               TRANSACTIONS.txn_type_code, TRANSACTIONS.txn_type_category, TRANSACTIONS.originator_name,
                               TRANSACTIONS.originator_address, TRANSACTIONS.originator_bank_country_code,
                               TRANSACTIONS.beneficiary_name, TRANSACTIONS.beneficiary_address,
                               TRANSACTIONS.beneficiary_bank_country_code, TRANSACTIONS.remittance_payment_details,
                               TRANSACTIONS.cheque_no, TRANSACTIONS.teller_id, TRANSACTIONS.your_reference,
                               TRANSACTIONS.our_reference, TRANSACTIONS.bank_info, TRANSACTIONS.country_code,
                               TRANSACTIONS.merchant_name, TRANSACTIONS.atm_id, TRANSACTIONS.atm_location,
                               TRANSACTIONS.opp_account_key, TRANSACTIONS.opp_organisation_key,
                               TRANSACTIONS.opp_account_number, TRANSACTIONS.swift_msg_info,
                               TRANSACTIONS.swift_msg_type, TRANSACTIONS.card_number, TRANSACTIONS.opp_country_code,
                               TRANSACTIONS.tranche_no]

        self.txn_direction_mapping = {
            True: ["CDM-cash-deposit", "cash-equivalent-deposit", "incoming-cheque", "incoming-local-fund-transfer",
                   "cash-equivalent-card-payment", "card-payment",
                   "incoming-overseas-fund-transfer", "Misc-credit"],
            False: ["ATM-withdrawal", "cash-equivalent-withdrawal",
                    "card-charge", "outgoing-cheque", "outgoing-local-fund-transfer",
                    "outgoing-overseas-fund-transfer", "Misc-debit"]
        }

        # after all necessary joins, rename to match postpipeline table output
        self.rename_mapping = {
            TRANSACTIONS.transaction_key: UI_TRANSACTIONS.id,
            TRANSACTIONS.primary_party_key: UI_TRANSACTIONS.party_key,
            TRANSACTIONS.txn_date_time: UI_TRANSACTIONS.transaction_date,
            TRANSACTIONS.product_code: UI_TRANSACTIONS.product_code,
            TRANSACTIONS.acct_currency_code: UI_TRANSACTIONS.account_currency,
            TRANSACTIONS.acct_currency_amount: UI_TRANSACTIONS.amount,
            TRANSACTIONS.orig_currency_code: UI_TRANSACTIONS.transaction_currency,
            TRANSACTIONS.orig_currency_amount: UI_TRANSACTIONS.transaction_currency_amount,
            TRANSACTIONS.txn_amount: UI_TRANSACTIONS.local_currency_equivalent,
            ACCOUNTS.account_number: UI_TRANSACTIONS.account_id,
            TRANSACTIONS.txn_type_code: UI_TRANSACTIONS.transaction_type_code,
            TRANSACTIONS.txn_type_category: UI_TRANSACTIONS.transaction_type_description,
            TRANSACTIONS.originator_name: UI_TRANSACTIONS.originator,
            TRANSACTIONS.originator_address: UI_TRANSACTIONS.originator_address,
            TRANSACTIONS.originator_bank_country_code: UI_TRANSACTIONS.originator_bank_country_code,
            TRANSACTIONS.beneficiary_name: UI_TRANSACTIONS.beneficiary,
            TRANSACTIONS.beneficiary_address: UI_TRANSACTIONS.beneficiary_address,
            TRANSACTIONS.beneficiary_bank_country_code: UI_TRANSACTIONS.beneficiary_bank_country_code,
            TRANSACTIONS.remittance_payment_details: UI_TRANSACTIONS.remittance_payment_details,
            TRANSACTIONS.cheque_no: UI_TRANSACTIONS.cheque_no,
            TRANSACTIONS.teller_id: UI_TRANSACTIONS.teller_id,
            TRANSACTIONS.your_reference: UI_TRANSACTIONS.your_reference,
            TRANSACTIONS.our_reference: UI_TRANSACTIONS.our_reference,
            TRANSACTIONS.bank_info: UI_TRANSACTIONS.bank_info,
            TRANSACTIONS.country_code: UI_TRANSACTIONS.country_of_transaction,
            TRANSACTIONS.merchant_name: UI_TRANSACTIONS.merchant_name,
            TRANSACTIONS.atm_id: UI_TRANSACTIONS.atm_id,
            TRANSACTIONS.atm_location: UI_TRANSACTIONS.atm_location,
            TRANSACTIONS.swift_msg_type: UI_TRANSACTIONS.swift_msg_type,
            TRANSACTIONS.swift_msg_info: UI_TRANSACTIONS.swift_msg_info,
            FIXEDDEPOSITS.fd_td_number: UI_TRANSACTIONS.td_number,
            TRANSACTIONS.card_number: UI_TRANSACTIONS.credit_card_number,
            TRANSACTIONS.tranche_no: UI_TRANSACTIONS.tranche_no
        }

        # columns that are not available in any tables and need to be filled with below values
        self.derive_columns = {
            UI_TRANSACTIONS.remittance_country: TRANSACTIONS.opp_country_code
        }

        # columns that are not available in any tables and need to be filled with below values
        self.fill_columns = {
            UI_TRANSACTIONS.mcc: [None, Defaults.TYPE_STRING]
        }


class ConfigDeleteForRepriority:
    """
    ConfigDeleteForRepriority: configuration for DeleteForRepriority
    """
    cols_from_input_alerts = [CDDALERTS.alert_id]


INTERNAL_BROADCAST_LIMIT_MB = 700.0
DYN_PROPERTY_CATEGORY_NAME = "UDF_CATEGORY"
DYN_PRODUCT_MAPPING = "PRODUCT_MAPPING"
DYN_TXN_CODE_MAPPING = "TXN_CODE_MAPPING"
DYN_RISK_LEVEL_MAPPING = "RISK_LEVEL_MAPPING"
JSON_RETURN_TYPE = "json"
L3_STRING = "L3"
