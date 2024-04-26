try:
    from crs_prepipeline_tables import ACCOUNTS, TRANSACTIONS, FIXEDDEPOSITS, CUSTOMERS, HIGHRISKPARTY
    from crs_postpipeline_tables import UI_TRANSACTIONS
    from crs_postpipeline_tables import CRS_CUSTOMERS
    from crs_ui_mapping_configurations import ConfigCreateUITransactions
except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables import ACCOUNTS, TRANSACTIONS, \
        FIXEDDEPOSITS, CUSTOMERS, HIGHRISKPARTY
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_TRANSACTIONS
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_CUSTOMERS
    from CustomerRiskScoring.config.crs_ui_mapping_configurations import ConfigCreateUITransactions


class ConfigCRSCustomers:
    """
    ConfigCRSCustomers: configuration for ConfigCRSCustomers
    """

    def __init__(self):
        self.cols_from_customers = [CUSTOMERS.party_key, CUSTOMERS.party_name, CUSTOMERS.date_of_birth_or_incorporation,
                                    CUSTOMERS.acquisition_date, CUSTOMERS.individual_corporate_type,
                                    CUSTOMERS.industry_sector, CUSTOMERS.status_code, CUSTOMERS.customer_division,
                                    CUSTOMERS.nature_of_business, CUSTOMERS.residential_address, CUSTOMERS.postal_code,
                                    CUSTOMERS.customer_contact_no, CUSTOMERS.designation, CUSTOMERS.gender,
                                    CUSTOMERS.citizenship_country, CUSTOMERS.customer_id_no, CUSTOMERS.customer_id_type,
                                    CUSTOMERS.customer_id_country, CUSTOMERS.employer_name, CUSTOMERS.pep_flag,
                                    CUSTOMERS.employee_flag, CUSTOMERS.annual_turnover,
                                    CUSTOMERS.birth_incorporation_country, CUSTOMERS.residence_operation_country]

        self.cols_from_high_risk_party = [HIGHRISKPARTY.party_key, HIGHRISKPARTY.high_risk_start_date,
                                          HIGHRISKPARTY.high_risk_expiry_date, HIGHRISKPARTY.high_risk_reason]

        # after all necessary joins, rename to match postpipeline table output
        self.rename_mapping = {
            CUSTOMERS.party_key: CRS_CUSTOMERS.customer_key,
            CUSTOMERS.party_name: CRS_CUSTOMERS.customer_name,
            CUSTOMERS.date_of_birth_or_incorporation: CRS_CUSTOMERS.date_of_birth_incorporation,
            CUSTOMERS.acquisition_date: CRS_CUSTOMERS.customer_onboard_date,
            CUSTOMERS.individual_corporate_type: CRS_CUSTOMERS.customer_type,
            CUSTOMERS.industry_sector: CRS_CUSTOMERS.customer_industry,
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
            CUSTOMERS.residence_operation_country: CRS_CUSTOMERS.residence_operation_country,
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


class ConfigCRSTransactions(ConfigCreateUITransactions):
    """
    ConfigCreateUITransactions: configuration for CreateUITransactions
    """
    def __init__(self):
        super().__init__()


class ConfigCRSCustomerHistory:
    """
    ConfigCreateUITransactions: configuration for CreateUITransactions
    """

    def __init__(self):
        self.cols_from_delta_customers = [CUSTOMERS.party_key, CUSTOMERS.risk_score, CUSTOMERS.customer_segment_code,
                                          CUSTOMERS.customer_segment_name, CUSTOMERS.party_name]
