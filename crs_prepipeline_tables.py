from pyspark.sql.types import *

class CDDALERTS:
    # CDDALERTS TABLE
    # please add column variable for new columns

    alert_id = 'ALERT_ID'
    alert_created_date = 'ALERT_CREATED_DATE'
    alert_closed_date = 'ALERT_CLOSED_DATE'
    rule_name = 'RULE_NAME'
    rule_id = 'RULE_ID'
    rule_score = 'RULE_SCORE'
    alert_investigation_result = 'ALERT_INVESTIGATION_RESULT'
    alert_score = 'ALERT_SCORE'
    rule_score_details = 'RULE_SCORE_DETAILS'
    party_key = 'PARTY_KEY'
    population_code = 'POPULATION_CODE'
    alert_category = 'ALERT_CATEGORY'
    bank_analyst_user_name = 'BANK_ANALYST_USER_NAME'
    alert_bu_type = 'ALERT_BU_TYPE'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(alert_created_date, TimestampType()),
        StructField(alert_closed_date, TimestampType()),
        StructField(rule_name, StringType()),
        StructField(rule_id, StringType()),
        StructField(rule_score, StringType()),
        StructField(alert_investigation_result, StringType()),
        StructField(alert_score, StringType()),
        StructField(rule_score_details, StringType()),
        StructField(party_key, StringType()),
        StructField(population_code, StringType()),
        StructField(alert_category, StringType()),
        StructField(bank_analyst_user_name, StringType()),
        StructField(alert_bu_type, StringType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class CUSTOMERS:
    # CUSTOMERS TABLE
    # please add column variable for new columns

    party_key = 'PARTY_KEY'
    individual_corporate_type = 'INDIVIDUAL_CORPORATE_TYPE'
    entity_type = 'ENTITY_TYPE'
    entity_type_desc = 'ENTITY_TYPE_DESC'
    status_code = 'STATUS_CODE'
    party_name = 'PARTY_NAME'
    name_title = 'NAME_TITLE'
    first_name = 'FIRST_NAME'
    last_names = 'LAST_NAMES'
    previous_name = 'PREVIOUS_NAME'
    abbreviated_name = 'ABBREVIATED_NAME'
    customer_aliases = 'CUSTOMER_ALIASES'
    date_of_birth_or_incorporation = 'DATE_OF_BIRTH_OR_INCORPORATION'
    customer_id_no = 'CUSTOMER_ID_NO'
    customer_id_type = 'CUSTOMER_ID_TYPE'
    customer_id_country = 'CUSTOMER_ID_COUNTRY'
    id_expiry_date = 'ID_EXPIRY_DATE'
    registered_number = 'REGISTERED_NUMBER'
    citizenship_country = 'CITIZENSHIP_COUNTRY'
    domicile_country = 'DOMICILE_COUNTRY'
    residence_operation_country = 'RESIDENCE_OPERATION_COUNTRY'
    birth_incorporation_country = 'BIRTH_INCORPORATION_COUNTRY'
    country_of_origin = 'COUNTRY_OF_ORIGIN'
    place_of_birth = 'PLACE_OF_BIRTH'
    residential_address = 'RESIDENTIAL_ADDRESS'
    postal_code = 'POSTAL_CODE'
    company_form = 'COMPANY_FORM'
    zone = 'ZONE'
    gender = 'GENDER'
    race = 'RACE'
    deceased_date = 'DECEASED_DATE'
    deceased_flag = 'DECEASED_FLAG'
    marital_status = 'MARITAL_STATUS'
    business_profession = 'BUSINESS_PROFESSION'
    customer_segment_code = 'CUSTOMER_SEGMENT_CODE'
    customer_segment_name = 'CUSTOMER_SEGMENT_NAME'
    business_type = 'BUSINESS_TYPE'
    nature_of_business = 'NATURE_OF_BUSINESS'
    occupation = 'OCCUPATION'
    employment_status = 'EMPLOYMENT_STATUS'
    employee_type = 'EMPLOYEE_TYPE'
    employer_name = 'EMPLOYER_NAME'
    info_type_value = 'INFO_TYPE_VALUE'
    customer_contact_no = 'CUSTOMER_CONTACT_NO'
    major_buyer = 'MAJOR_BUYER'
    major_seller = 'MAJOR_SELLER'
    major_buyer_ctry = 'MAJOR_BUYER_CTRY'
    major_supplier_ctry = 'MAJOR_SUPPLIER_CTRY'
    major_buyer_industry_cd = 'MAJOR_BUYER_INDUSTRY_CD'
    major_supplier_industry_cd = 'MAJOR_SUPPLIER_INDUSTRY_CD'
    relationship_mgr_id = 'RELATIONSHIP_MGR_ID'
    customer_division = 'CUSTOMER_DIVISION'
    customer_orgunit_code = 'CUSTOMER_ORGUNIT_CODE'
    overall_score_adjustment = 'OVERALL_SCORE_ADJUSTMENT'
    risk_score = 'RISK_SCORE'
    risk_level = 'RISK_LEVEL'
    alert_info_rsn = 'ALERT_INFO_RSN'
    pep_flag = 'PEP_FLAG'
    pep_not_on_watchlist = 'PEP_NOT_ON_WATCHLIST'
    pep_by_account_association = 'PEP_BY_ACCOUNT_ASSOCIATION'
    employee_flag = 'EMPLOYEE_FLAG'
    foreign_financial_org_flag = 'FOREIGN_FINANCIAL_ORG_FLAG'
    foreign_official_flag = 'FOREIGN_OFFICIAL_FLAG'
    non_physical_address_flag = 'NON_PHYSICAL_ADDRESS_FLAG'
    residence_flag = 'RESIDENCE_FLAG'
    special_attention_flag = 'SPECIAL_ATTENTION_FLAG'
    incorporate_taxhaven_flag = 'INCORPORATE_TAXHAVEN_FLAG'
    bankrupt_flag = 'BANKRUPT_FLAG'
    near_border_flag = 'NEAR_BORDER_FLAG'
    adverse_news_flag = 'ADVERSE_NEWS_FLAG'
    weak_aml_ctrl_flag = 'WEAK_AML_CTRL_FLAG'
    cash_intensive_business_flag = 'CASH_INTENSIVE_BUSINESS_FLAG'
    correspondent_bank_flag = 'CORRESPONDENT_BANK_FLAG'
    money_service_bureau_flag = 'MONEY_SERVICE_BUREAU_FLAG'
    non_bank_finance_institute_flag = 'NON_BANK_FINANCE_INSTITUTE_FLAG'
    wholesale_banknote_flag = 'WHOLESALE_BANKNOTE_FLAG'
    compensation_reqd_flag = 'COMPENSATION_REQD_FLAG'
    complaint_flag = 'COMPLAINT_FLAG'
    end_relationship_flag = 'END_RELATIONSHIP_FLAG'
    face_to_face_flag = 'FACE_TO_FACE_FLAG'
    ngo_flag = 'NGO_FLAG'
    high_risk_country_flag = 'HIGH_RISK_COUNTRY_FLAG'
    acquisition_date = 'ACQUISITION_DATE'
    balance_sheet_total = 'BALANCE_SHEET_TOTAL'
    annual_turnover = 'ANNUAL_TURNOVER'
    trading_duration = 'TRADING_DURATION'
    prime_branch_id = 'PRIME_BRANCH_ID'
    merchant_number = 'MERCHANT_NUMBER'
    channel = 'CHANNEL'
    intended_product_use = 'INTENDED_PRODUCT_USE'
    complex_structure = 'COMPLEX_STRUCTURE'
    dormant_override_date = 'DORMANT_OVERRIDE_DATE'
    customer_uniqueness_score = 'CUSTOMER_UNIQUENESS_SCORE'
    designation = 'DESIGNATION'
    party_corporate_structure = 'PARTY_CORPORATE_STRUCTURE'
    country_of_jurisdiction = 'COUNTRY_OF_JURISDICTION'
    source_of_funds = 'SOURCE_OF_FUNDS'
    nature_of_business_relation = 'NATURE_OF_BUSINESS_RELATION'
    industry_sector = 'INDUSTRY_SECTOR'
    industry_risk = 'INDUSTRY_RISK'
    annual_revenue_or_income = 'ANNUAL_REVENUE_OR_INCOME'
    bureau_score = 'BUREAU_SCORE'
    country_of_financial_interest = 'COUNTRY_OF_FINANCIAL_INTEREST'
    high_net_worth_flag = 'HIGH_NET_WORTH_FLAG'
    criminal_offence_flag = 'CRIMINAL_OFFENCE_FLAG'
    purpose_of_account_opening = 'PURPOSE_OF_ACCOUNT_OPENING'
    product_risk = 'PRODUCT_RISK'
    first_time_business_relationship = 'FIRST_TIME_BUSINESS_RELATIONSHIP'
    business_registration_document_expiry_date = 'BUSINESS_REGISTRATION_DOCUMENT_EXPIRY_DATE'
    copy_of_business_registration_document = 'COPY_OF_BUSINESS_REGISTRATION_DOCUMENT'
    shareholding_structure_info_completion = 'SHAREHOLDING_STRUCTURE_INFO_COMPLETION'
    shareholding_structure_layer = 'SHAREHOLDING_STRUCTURE_LAYER'
    beneficial_owner_info_completion = 'BENEFICIAL_OWNER_INFO_COMPLETION'
    directors_representatives_info_completion = 'DIRECTORS_REPRESENTATIVES_INFO_COMPLETION'
    business_relationship_establishing_method = 'BUSINESS_RElATIONSHIP_ESTABLISHING_METHOD'
    business_size_high_rank_flag = 'BUSINESS_SIZE_HIGH_RANK_FLAG'
    number_of_payment_sub_channels = 'NUMBER_OF_PAYMENT_SUB_CHANNELS'
    expected_single_transaction_amount = 'EXPECTED_SINGLE_TRANSACTION_AMOUNT'
    regulatory_request = 'REGULATORY_REQUEST'
    bearer_share_flag = 'BEARER_SHARE_FLAG'
    has_evaded_tax = 'HAS_EVADED_TAX'
    public_company_flag = 'PUBLIC_COMPANY_FLAG'
    offshore_company_flag = 'OFFSHORE_COMPANY_FLAG'
    kyc_declared_withdrawal_limit = 'KYC_DECLARED_WITHDRAWAL_LIMIT'
    kyc_declared_deposit_limit = 'KYC_DECLARED_DEPOSIT_LIMIT'
    periodic_review_flag = 'PERIODIC_REVIEW_FLAG'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_deleted = 'TT_IS_DELETED'
    party_type = "PARTY_TYPE"

    schema = StructType([
        StructField(party_key, StringType()),
        StructField(individual_corporate_type, StringType()),
        StructField(entity_type, StringType()),
        StructField(entity_type_desc, StringType()),
        StructField(status_code, StringType()),
        StructField(party_name, StringType()),
        StructField(name_title, StringType()),
        StructField(first_name, StringType()),
        StructField(last_names, StringType()),
        StructField(previous_name, StringType()),
        StructField(abbreviated_name, StringType()),
        StructField(customer_aliases, StringType()),
        StructField(date_of_birth_or_incorporation, TimestampType()),
        StructField(customer_id_no, StringType()),
        StructField(customer_id_type, StringType()),
        StructField(customer_id_country, StringType()),
        StructField(id_expiry_date, TimestampType()),
        StructField(registered_number, StringType()),
        StructField(citizenship_country, StringType()),
        StructField(domicile_country, StringType()),
        StructField(residence_operation_country, StringType()),
        StructField(birth_incorporation_country, StringType()),
        StructField(country_of_origin, StringType()),
        StructField(place_of_birth, StringType()),
        StructField(residential_address, StringType()),
        StructField(postal_code, StringType()),
        StructField(company_form, StringType()),
        StructField(zone, StringType()),
        StructField(gender, StringType()),
        StructField(race, StringType()),
        StructField(deceased_date, TimestampType()),
        StructField(deceased_flag, BooleanType()),
        StructField(marital_status, StringType()),
        StructField(business_profession, StringType()),
        StructField(customer_segment_code, StringType()),
        StructField(customer_segment_name, StringType()),
        StructField(business_type, StringType()),
        StructField(nature_of_business, StringType()),
        StructField(occupation, StringType()),
        StructField(employment_status, StringType()),
        StructField(employee_type, StringType()),
        StructField(employer_name, StringType()),
        StructField(info_type_value, StringType()),
        StructField(customer_contact_no, StringType()),
        StructField(major_buyer, StringType()),
        StructField(major_seller, StringType()),
        StructField(major_buyer_ctry, StringType()),
        StructField(major_supplier_ctry, StringType()),
        StructField(major_buyer_industry_cd, StringType()),
        StructField(major_supplier_industry_cd, StringType()),
        StructField(relationship_mgr_id, StringType()),
        StructField(customer_division, StringType()),
        StructField(customer_orgunit_code, StringType()),
        StructField(overall_score_adjustment, DoubleType()),
        StructField(risk_score, DoubleType()),
        StructField(risk_level, StringType()),
        StructField(alert_info_rsn, StringType()),
        StructField(pep_flag, BooleanType()),
        StructField(pep_not_on_watchlist, StringType()),
        StructField(pep_by_account_association, StringType()),
        StructField(employee_flag, BooleanType()),
        StructField(foreign_financial_org_flag, BooleanType()),
        StructField(foreign_official_flag, BooleanType()),
        StructField(non_physical_address_flag, BooleanType()),
        StructField(residence_flag, BooleanType()),
        StructField(special_attention_flag, BooleanType()),
        StructField(incorporate_taxhaven_flag, BooleanType()),
        StructField(bankrupt_flag, BooleanType()),
        StructField(near_border_flag, BooleanType()),
        StructField(adverse_news_flag, BooleanType()),
        StructField(weak_aml_ctrl_flag, BooleanType()),
        StructField(cash_intensive_business_flag, BooleanType()),
        StructField(correspondent_bank_flag, BooleanType()),
        StructField(money_service_bureau_flag, BooleanType()),
        StructField(non_bank_finance_institute_flag, BooleanType()),
        StructField(wholesale_banknote_flag, BooleanType()),
        StructField(compensation_reqd_flag, BooleanType()),
        StructField(complaint_flag, BooleanType()),
        StructField(end_relationship_flag, BooleanType()),
        StructField(face_to_face_flag, BooleanType()),
        StructField(ngo_flag, BooleanType()),
        StructField(high_risk_country_flag, BooleanType()),
        StructField(acquisition_date, TimestampType()),
        StructField(balance_sheet_total, DoubleType()),
        StructField(annual_turnover, DoubleType()),
        StructField(trading_duration, IntegerType()),
        StructField(prime_branch_id, StringType()),
        StructField(merchant_number, StringType()),
        StructField(channel, StringType()),
        StructField(intended_product_use, StringType()),
        StructField(complex_structure, StringType()),
        StructField(dormant_override_date, TimestampType()),
        StructField(customer_uniqueness_score, DoubleType()),
        StructField(designation, StringType()),
        StructField(party_corporate_structure, StringType()),
        StructField(country_of_jurisdiction, StringType()),
        StructField(source_of_funds, StringType()),
        StructField(nature_of_business_relation, StringType()),
        StructField(industry_sector, StringType()),
        StructField(industry_risk, BooleanType()),
        StructField(annual_revenue_or_income, DoubleType()),
        StructField(bureau_score, DoubleType()),
        StructField(country_of_financial_interest, StringType()),
        StructField(high_net_worth_flag, BooleanType()),
        StructField(criminal_offence_flag, BooleanType()),
        StructField(purpose_of_account_opening, StringType()),
        StructField(product_risk, BooleanType()),
        StructField(first_time_business_relationship, BooleanType()),
        StructField(business_registration_document_expiry_date, TimestampType()),
        StructField(copy_of_business_registration_document, BooleanType()),
        StructField(shareholding_structure_info_completion, BooleanType()),
        StructField(shareholding_structure_layer, IntegerType()),
        StructField(beneficial_owner_info_completion, BooleanType()),
        StructField(directors_representatives_info_completion, BooleanType()),
        StructField(business_relationship_establishing_method, StringType()),
        StructField(business_size_high_rank_flag, BooleanType()),
        StructField(number_of_payment_sub_channels, IntegerType()),
        StructField(expected_single_transaction_amount, IntegerType()),
        StructField(regulatory_request, BooleanType()),
        StructField(bearer_share_flag, BooleanType()),
        StructField(has_evaded_tax, BooleanType()),
        StructField(public_company_flag, BooleanType()),
        StructField(offshore_company_flag, BooleanType()),
        StructField(kyc_declared_withdrawal_limit, DoubleType()),
        StructField(kyc_declared_deposit_limit, DoubleType()),
        StructField(periodic_review_flag, BooleanType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_latest_data, BooleanType()),
        StructField(tt_is_deleted, StringType()),
        StructField(etl_custom_column, BooleanType()),
    ])


class ACCOUNTS:
    # ACCOUNTS TABLE
    # please add column variable for new columns

    account_key = 'ACCOUNT_KEY'
    account_number = 'ACCOUNT_NUMBER'
    status_code = 'STATUS_CODE'
    open_date = 'OPEN_DATE'
    type_code = 'TYPE_CODE'
    segment_code = 'SEGMENT_CODE'
    business_unit = 'BUSINESS_UNIT'
    country_code = 'COUNTRY_CODE'
    mail_code = 'MAIL_CODE'
    undelivered_mail_flag = 'UNDELIVERED_MAIL_FLAG'
    primary_party_key = 'PRIMARY_PARTY_KEY'
    risk_level = 'RISK_LEVEL'
    sector_code = 'SECTOR_CODE'
    currency_code = 'CURRENCY_CODE'
    closed_date = 'CLOSED_DATE'
    credit_limit = 'CREDIT_LIMIT'
    account_name = 'ACCOUNT_NAME'
    branch_id = 'BRANCH_ID'
    branch_location = 'BRANCH_LOCATION'
    opening_balance = 'OPENING_BALANCE'
    avg_debit_balance = 'AVG_DEBIT_BALANCE'
    avg_credit_balance = 'AVG_CREDIT_BALANCE'
    account_risk_score = 'ACCOUNT_RISK_SCORE'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(account_key, StringType()),
        StructField(account_number, StringType()),
        StructField(status_code, StringType()),
        StructField(open_date, TimestampType()),
        StructField(type_code, StringType()),
        StructField(segment_code, StringType()),
        StructField(business_unit, StringType()),
        StructField(country_code, StringType()),
        StructField(mail_code, StringType()),
        StructField(undelivered_mail_flag, BooleanType()),
        StructField(primary_party_key, StringType()),
        StructField(risk_level, StringType()),
        StructField(sector_code, StringType()),
        StructField(currency_code, StringType()),
        StructField(closed_date, TimestampType()),
        StructField(credit_limit, DoubleType()),
        StructField(account_name, StringType()),
        StructField(branch_id, StringType()),
        StructField(branch_location, StringType()),
        StructField(opening_balance, DoubleType()),
        StructField(avg_debit_balance, DoubleType()),
        StructField(avg_credit_balance, DoubleType()),
        StructField(account_risk_score, DoubleType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class TRANSACTIONS:
    # TRANSACTIONS TABLE
    # please add column variable for new columns

    transaction_key = 'TRANSACTION_KEY'
    primary_party_key = 'PRIMARY_PARTY_KEY'
    account_key = 'ACCOUNT_KEY'
    merchant_name = 'MERCHANT_NAME'
    card_number = 'CARD_NUMBER'
    txn_date_time = 'TXN_DATE_TIME'
    txn_type_code = 'TXN_TYPE_CODE'
    txn_type_category = 'TXN_TYPE_CATEGORY'
    txn_direction = 'TXN_DIRECTION'
    orig_txn_type_code = 'ORIG_TXN_TYPE_CODE'
    orig_currency_code = 'ORIG_CURRENCY_CODE'
    orig_currency_amount = 'ORIG_CURRENCY_AMOUNT'
    acct_currency_code = 'ACCT_CURRENCY_CODE'
    acct_currency_amount = 'ACCT_CURRENCY_AMOUNT'
    base_currency = 'BASE_CURRENCY'
    txn_amount = 'TXN_AMOUNT'
    country_code = 'COUNTRY_CODE'
    opp_account_number = 'OPP_ACCOUNT_NUMBER'
    opp_organisation_key = 'OPP_ORGANISATION_KEY'
    opp_country_code = 'OPP_COUNTRY_CODE'
    opp_account_key = 'OPP_ACCOUNT_KEY'
    originator_name = 'ORIGINATOR_NAME'
    beneficiary_name = 'BENEFICIARY_NAME'
    swift_msg_info = 'SWIFT_MSG_INFO'
    swift_msg_type = 'SWIFT_MSG_TYPE'
    originator_address = 'ORIGINATOR_ADDRESS'
    originator_bank_country_code = 'ORIGINATOR_BANK_COUNTRY_CODE'
    beneficiary_address = 'BENEFICIARY_ADDRESS'
    beneficiary_bank_country_code = 'BENEFICIARY_BANK_COUNTRY_CODE'
    atm_id = 'ATM_ID'
    atm_location = 'ATM_LOCATION'
    remittance_function_id = 'REMITTANCE_FUNCTION_ID'
    remittance_payment_mode = 'REMITTANCE_PAYMENT_MODE'
    remittance_txn_number = 'REMITTANCE_TXN_NUMBER'
    remittance_payment_details = 'REMITTANCE_PAYMENT_DETAILS'
    bank_branch_code = 'BANK_BRANCH_CODE'
    cheque_no = 'CHEQUE_NO'
    teller_id = 'TELLER_ID'
    your_reference = 'YOUR_REFERENCE'
    our_reference = 'OUR_REFERENCE'
    bank_info = 'BANK_INFO'
    product_code = 'PRODUCT_CODE'
    loan_key = 'LOAN_KEY'
    opp_account_type = 'OPP_ACCOUNT_TYPE'
    aft_txn_acct_balance = 'AFT_TXN_ACCT_BALANCE'
    exchange_rate = 'EXCHANGE_RATE'
    include_exclude_flag = 'INCLUDE_EXCLUDE_FLAG'
    org_amount_flag = 'ORG_AMOUNT_FLAG'
    negative_amount_flag = 'NEGATIVE_AMOUNT_FLAG'
    purpose_code = 'PURPOSE_CODE'
    tranche_no = 'TRANCHE_NO'
    tt_created_time = 'TT_CREATED_TIME'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'

    schema = StructType([
        StructField(transaction_key, StringType()),
        StructField(primary_party_key, StringType()),
        StructField(account_key, StringType()),
        StructField(merchant_name, StringType()),
        StructField(card_number, StringType()),
        StructField(txn_date_time, TimestampType()),
        StructField(txn_type_code, StringType()),
        StructField(txn_type_category, StringType()),
        StructField(txn_direction, StringType()),
        StructField(orig_txn_type_code, StringType()),
        StructField(orig_currency_code, StringType()),
        StructField(orig_currency_amount, DoubleType()),
        StructField(acct_currency_code, StringType()),
        StructField(acct_currency_amount, DoubleType()),
        StructField(base_currency, StringType()),
        StructField(txn_amount, DoubleType()),
        StructField(country_code, StringType()),
        StructField(opp_account_number, StringType()),
        StructField(opp_organisation_key, StringType()),
        StructField(opp_country_code, StringType()),
        StructField(opp_account_key, StringType()),
        StructField(originator_name, StringType()),
        StructField(beneficiary_name, StringType()),
        StructField(swift_msg_info, StringType()),
        StructField(swift_msg_type, StringType()),
        StructField(originator_address, StringType()),
        StructField(originator_bank_country_code, StringType()),
        StructField(beneficiary_address, StringType()),
        StructField(beneficiary_bank_country_code, StringType()),
        StructField(atm_id, StringType()),
        StructField(atm_location, StringType()),
        StructField(remittance_function_id, StringType()),
        StructField(remittance_payment_mode, StringType()),
        StructField(remittance_txn_number, StringType()),
        StructField(remittance_payment_details, StringType()),
        StructField(bank_branch_code, StringType()),
        StructField(cheque_no, StringType()),
        StructField(teller_id, StringType()),
        StructField(your_reference, StringType()),
        StructField(our_reference, StringType()),
        StructField(bank_info, StringType()),
        StructField(product_code, StringType()),
        StructField(loan_key, StringType()),
        StructField(opp_account_type, StringType()),
        StructField(aft_txn_acct_balance, DoubleType()),
        StructField(exchange_rate, DoubleType()),
        StructField(include_exclude_flag, BooleanType()),
        StructField(org_amount_flag, BooleanType()),
        StructField(negative_amount_flag, BooleanType()),
        StructField(purpose_code, StringType()),
        StructField(tranche_no, StringType()),
        StructField(tt_created_time, TimestampType()),
        StructField(etl_custom_column, StringType()),
    ])


class TMALERTS:
    # ALERTS TABLE
    # please add column variable for new columns

    alert_id = 'ALERT_ID'
    account_key = 'ACCOUNT_KEY'
    alert_created_date = 'ALERT_CREATED_DATE'
    alert_closed_date = 'ALERT_CLOSED_DATE'
    rule_name = 'RULE_NAME'
    rule_id = 'RULE_ID'
    rule_score = 'RULE_SCORE'
    alert_investigation_result = 'ALERT_INVESTIGATION_RESULT'
    alert_score = 'ALERT_SCORE'
    rule_score_details = 'RULE_SCORE_DETAILS'
    party_key = 'PARTY_KEY'
    population_code = 'POPULATION_CODE'
    alert_category = 'ALERT_CATEGORY'
    bank_analyst_user_name = 'BANK_ANALYST_USER_NAME'
    alert_bu_type = 'ALERT_BU_TYPE'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(account_key, StringType()),
        StructField(alert_created_date, TimestampType()),
        StructField(alert_closed_date, TimestampType()),
        StructField(rule_name, StringType()),
        StructField(rule_id, StringType()),
        StructField(rule_score, StringType()),
        StructField(alert_investigation_result, StringType()),
        StructField(alert_score, StringType()),
        StructField(rule_score_details, StringType()),
        StructField(party_key, StringType()),
        StructField(population_code, StringType()),
        StructField(alert_category, StringType()),
        StructField(bank_analyst_user_name, StringType()),
        StructField(alert_bu_type, StringType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class C2C:
    # CUSTOMERS TO CUSTOMERS TABLE
    # please add column variable for new columns

    party_key = 'PARTY_KEY'
    linked_party_key = 'LINKED_PARTY_KEY'
    shareholder_percentage = 'SHAREHOLDER_PERCENTAGE'
    relationship_description = 'RELATIONSHIP_DESCRIPTION'
    relation_code = 'RELATION_CODE'
    relationship_end_date = 'RELATIONSHIP_END_DATE'
    linked_party_name = 'LINKED_PARTY_NAME'
    linked_party_country = 'LINKED_PARTY_COUNTRY'
    linked_party_risk_score = 'LINKED_PARTY_RISK_SCORE'
    captital_contributed_amount = 'CAPTITAL_CONTRIBUTED_AMOUNT'
    profit_sharing_ratio = 'PROFIT_SHARING_RATIO'
    linked_party_city = 'LINKED_PARTY_CITY'
    linked_party_address = 'LINKED_PARTY_ADDRESS'
    purpose_of_relation = 'PURPOSE_OF_RELATION'
    public_company_flag = 'PUBLIC_COMPANY_FLAG'
    linked_party_pep_flag = 'LINKED_PARTY_PEP_FLAG'
    linked_party_adverse_news_flag = 'LINKED_PARTY_ADVERSE_NEWS_FLAG'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(party_key, StringType()),
        StructField(linked_party_key, StringType()),
        StructField(shareholder_percentage, StringType()),
        StructField(relationship_description, StringType()),
        StructField(relation_code, StringType()),
        StructField(relationship_end_date, TimestampType()),
        StructField(linked_party_name, StringType()),
        StructField(linked_party_country, StringType()),
        StructField(linked_party_risk_score, DoubleType()),
        StructField(captital_contributed_amount, DoubleType()),
        StructField(profit_sharing_ratio, DoubleType()),
        StructField(linked_party_city, StringType()),
        StructField(linked_party_address, StringType()),
        StructField(purpose_of_relation, StringType()),
        StructField(public_company_flag, BooleanType()),
        StructField(linked_party_pep_flag, BooleanType()),
        StructField(linked_party_adverse_news_flag, BooleanType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class C2A:
    # CUSTOMERS TO ACCOUNTS TABLE
    # please add column variable for new columns

    party_key = 'PARTY_KEY'
    account_key = 'ACCOUNT_KEY'
    relation_code = 'RELATION_CODE'
    relationship_end_date = 'RELATIONSHIP_END_DATE'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(party_key, StringType()),
        StructField(account_key, StringType()),
        StructField(relation_code, StringType()),
        StructField(relationship_end_date, TimestampType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class HIGHRISKCOUNTRY:
    # HIGH RISK COUNTRY TABLE
    # please add column variable for new columns

    country_code = 'COUNTRY_CODE'
    country_name = 'COUNTRY_NAME'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(country_code, StringType()),
        StructField(country_name, StringType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class HIGHRISKPARTY:
    # HIGH RISK PARTY TABLE
    # please add column variable for new columns

    party_key = 'PARTY_KEY'
    high_risk_start_date = 'HIGH_RISK_START_DATE'
    high_risk_expiry_date = 'HIGH_RISK_EXPIRY_DATE'
    high_risk_additional_info = 'HIGH_RISK_ADDITIONAL_INFO'
    high_risk_reason = 'HIGH_RISK_REASON'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(party_key, StringType()),
        StructField(high_risk_start_date, TimestampType()),
        StructField(high_risk_expiry_date, TimestampType()),
        StructField(high_risk_additional_info, StringType()),
        StructField(high_risk_reason, StringType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class CUSTOMERADDRESS:
    # CUSTOMER_ADDRESSES TABLE
    # please add column variable for new columns

    party_key = 'PARTY_KEY'
    address_id = 'ADDRESS_ID'
    address_type = 'ADDRESS_TYPE'
    address_1 = 'ADDRESS_1'
    address_2 = 'ADDRESS_2'
    address_3 = 'ADDRESS_3'
    address_4 = 'ADDRESS_4'
    city = 'CITY'
    postal_code = 'POSTAL_CODE'
    state = 'STATE'
    country = 'COUNTRY'
    foreign_country_flag = 'FOREIGN_COUNTRY_FLAG'
    undeliver_mail_flag = 'UNDELIVER_MAIL_FLAG'
    last_updated_timestamp = 'LAST_UPDATED_TIMESTAMP'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(party_key, StringType()),
        StructField(address_id, StringType()),
        StructField(address_type, StringType()),
        StructField(address_1, StringType()),
        StructField(address_2, StringType()),
        StructField(address_3, StringType()),
        StructField(address_4, StringType()),
        StructField(city, StringType()),
        StructField(postal_code, StringType()),
        StructField(state, StringType()),
        StructField(country, StringType()),
        StructField(foreign_country_flag, BooleanType()),
        StructField(undeliver_mail_flag, BooleanType()),
        StructField(last_updated_timestamp, TimestampType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class CUSTOMERCONTACT:
    # CUSTOMER_CONTACT TABLE
    # please add column variable for new columns

    party_key = 'PARTY_KEY'
    contact_id = 'CONTACT_ID'
    type_code = 'TYPE_CODE'
    type_desc = 'TYPE_DESC'
    number = 'NUMBER'
    contact_name = 'CONTACT_NAME'
    contact_department = 'CONTACT_DEPARTMENT'
    job_designation_code = 'JOB_DESIGNATION_CODE'
    job_designation = 'JOB_DESIGNATION'
    last_updated_timestamp = 'LAST_UPDATED_TIMESTAMP'
    contact_status = 'CONTACT_STATUS'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(party_key, StringType()),
        StructField(contact_id, StringType()),
        StructField(type_code, StringType()),
        StructField(type_desc, StringType()),
        StructField(number, StringType()),
        StructField(contact_name, StringType()),
        StructField(contact_department, StringType()),
        StructField(job_designation_code, StringType()),
        StructField(job_designation, StringType()),
        StructField(last_updated_timestamp, TimestampType()),
        StructField(contact_status, StringType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class FIXEDDEPOSITS:
    # FIXED DEPOSIT TABLE
    # please add column variable for new columns

    account_key = 'ACCOUNT_KEY'
    fd_key = 'FD_KEY'
    party_key = 'PARTY_KEY'
    fd_td_number = 'FD_TD_NUMBER'
    fd_original_amount = 'FD_ORIGINAL_AMOUNT'
    fd_date = 'FD_DATE'
    fd_maturity_date = 'FD_MATURITY_DATE'
    fd_category_code = 'FD_CATEGORY_CODE'
    fd_currency_code = 'FD_CURRENCY_CODE'
    fd_receipt_number = 'FD_RECEIPT_NUMBER'
    fd_status_code = 'FD_STATUS_CODE'
    fd_status_date = 'FD_STATUS_DATE'
    fd_renewal_counter = 'FD_RENEWAL_COUNTER'
    fd_pledge_flag = 'FD_PLEDGE_FLAG'
    fd_deposit_type_code = 'FD_DEPOSIT_TYPE_CODE'
    outstanding_fd_balance = 'OUTSTANDING_FD_BALANCE'
    fd_withdrawal_date = 'FD_WITHDRAWAL_DATE'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(account_key, StringType()),
        StructField(fd_key, StringType()),
        StructField(party_key, StringType()),
        StructField(fd_td_number, StringType()),
        StructField(fd_original_amount, DoubleType()),
        StructField(fd_date, TimestampType()),
        StructField(fd_maturity_date, TimestampType()),
        StructField(fd_category_code, StringType()),
        StructField(fd_currency_code, StringType()),
        StructField(fd_receipt_number, StringType()),
        StructField(fd_status_code, StringType()),
        StructField(fd_status_date, TimestampType()),
        StructField(fd_renewal_counter, StringType()),
        StructField(fd_pledge_flag, BooleanType()),
        StructField(fd_deposit_type_code, StringType()),
        StructField(outstanding_fd_balance, DoubleType()),
        StructField(fd_withdrawal_date, TimestampType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class LOANS:
    # LOANS TABLE
    # please add column variable for new columns

    loan_key = 'LOAN_KEY'
    account_key = 'ACCOUNT_KEY'
    anticipated_settlement_date = 'ANTICIPATED_SETTLEMENT_DATE'
    borrower_party_key = 'BORROWER_PARTY_KEY'
    loan_amount = 'LOAN_AMOUNT'
    loan_category_code = 'LOAN_CATEGORY_CODE'
    loan_currency_code = 'LOAN_CURRENCY_CODE'
    loan_start_date = 'LOAN_START_DATE'
    loan_status_code = 'LOAN_STATUS_CODE'
    loan_type_code = 'LOAN_TYPE_CODE'
    loan_type_description = 'LOAN_TYPE_DESCRIPTION'
    outstanding_loan_balance = 'OUTSTANDING_LOAN_BALANCE'
    installment_value = 'INSTALLMENT_VALUE'
    last_payment_date = 'LAST_PAYMENT_DATE'
    number_of_payments = 'NUMBER_OF_PAYMENTS'
    loan_purpose_code = 'LOAN_PURPOSE_CODE'
    gl_group = 'GL_GROUP'
    total_repayment_amount = 'TOTAL_REPAYMENT_AMOUNT'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(loan_key, StringType()),
        StructField(account_key, StringType()),
        StructField(anticipated_settlement_date, TimestampType()),
        StructField(borrower_party_key, StringType()),
        StructField(loan_amount, DoubleType()),
        StructField(loan_category_code, StringType()),
        StructField(loan_currency_code, StringType()),
        StructField(loan_start_date, TimestampType()),
        StructField(loan_status_code, StringType()),
        StructField(loan_type_code, StringType()),
        StructField(loan_type_description, StringType()),
        StructField(outstanding_loan_balance, DoubleType()),
        StructField(installment_value, DoubleType()),
        StructField(last_payment_date, TimestampType()),
        StructField(number_of_payments, IntegerType()),
        StructField(loan_purpose_code, StringType()),
        StructField(gl_group, DoubleType()),
        StructField(total_repayment_amount, DoubleType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class CURRENCYCONVERSIONRATE:
    # CURRENCYCONVERSIONRATE TABLE
    # please add column variable for new columns

    currency_code = 'CURRENCY_CODE'
    valid_date = 'VALID_DATE'
    base_currency_code = 'BASE_CURRENCY_CODE'
    exchange_rate = 'EXCHANGE_RATE'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(currency_code, StringType()),
        StructField(valid_date, TimestampType()),
        StructField(base_currency_code, StringType()),
        StructField(exchange_rate, DoubleType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class FIGMAPPING:
    # FIGMAPPING TABLE
    # please add column variable for new columns

    industry_code = 'INDUSTRY_CODE'
    industry_sector = 'INDUSTRY_SECTOR'
    industry_group = 'INDUSTRY_GROUP'
    industry_sub_segment_code = 'INDUSTRY_SUB_SEGMENT_CODE'
    fig_effective_date = 'FIG_EFFECTIVE_DATE'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(industry_code, StringType()),
        StructField(industry_sector, StringType()),
        StructField(industry_group, StringType()),
        StructField(industry_sub_segment_code, StringType()),
        StructField(fig_effective_date, StringType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class CODES:
    # CODES TABLE
    # please add column variable for new columns

    code = 'CODE'
    code_type = 'CODE_TYPE'
    description = 'DESCRIPTION'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(code, StringType()),
        StructField(code_type, StringType()),
        StructField(description, StringType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])


class CARDS:
    # CARDS TABLE
    # please add column variable for new columns

    card_key = 'CARD_KEY'
    tenant_cd = 'TENANT_CD'
    account_key = 'ACCOUNT_KEY'
    acct_curr_credit_limit = 'ACCT_CURR_CREDIT_LIMIT'
    base_curr_credit_limit = 'BASE_CURR_CREDIT_LIMIT'
    card_category_cd = 'CARD_CATEGORY_CD'
    card_classification_cd = 'CARD_CLASSIFICATION_CD'
    card_holder_party_key = 'CARD_HOLDER_PARTY_KEY'
    card_number = 'CARD_NUMBER'
    card_status_cd = 'CARD_STATUS_CD'
    card_status_date = 'CARD_STATUS_DATE'
    card_term_cd = 'CARD_TERM_CD'
    card_type_cd = 'CARD_TYPE_CD'
    expiration_date = 'EXPIRATION_DATE'
    is_to_be_deleted = 'IS_TO_BE_DELETED'
    issue_date = 'ISSUE_DATE'
    retailer_key = 'RETAILER_KEY'
    card_activation_date = 'CARD_ACTIVATION_DATE'
    card_atm_limit = 'CARD_ATM_LIMIT'
    card_bin = 'CARD_BIN'
    card_change_date = 'CARD_CHANGE_DATE'
    card_icc_card_ind = 'CARD_ICC_CARD_IND'
    card_pan_for_display = 'CARD_PAN_FOR_DISPLAY'
    card_pos_limit = 'CARD_POS_LIMIT'
    is_contact_chip_card = 'IS_CONTACT_CHIP_CARD'
    is_contactless_chip_card = 'IS_CONTACTLESS_CHIP_CARD'
    is_contactless_mag_emu_card = 'IS_CONTACTLESS_MAG_EMU_CARD'
    is_emv_card = 'IS_EMV_CARD'
    is_emv_cda_card = 'IS_EMV_CDA_CARD'
    is_mag_stripe_card = 'IS_MAG_STRIPE_CARD'
    pan_country_cd = 'PAN_COUNTRY_CD'
    tt_created_time = 'TT_CREATED_TIME'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'
    etl_custom_column = 'ETL_CUSTOM_COLUMN'
    tt_is_latest_data = 'TT_IS_LATEST_DATA'

    schema = StructType([
        StructField(card_key, StringType()),
        StructField(tenant_cd, StringType()),
        StructField(account_key, StringType()),
        StructField(acct_curr_credit_limit, DoubleType()),
        StructField(base_curr_credit_limit, DoubleType()),
        StructField(card_category_cd, StringType()),
        StructField(card_classification_cd, StringType()),
        StructField(card_holder_party_key, StringType()),
        StructField(card_number, StringType()),
        StructField(card_status_cd, StringType()),
        StructField(card_status_date, TimestampType()),
        StructField(card_term_cd, StringType()),
        StructField(card_type_cd, StringType()),
        StructField(expiration_date, TimestampType()),
        StructField(is_to_be_deleted, BooleanType()),
        StructField(issue_date, TimestampType()),
        StructField(retailer_key, StringType()),
        StructField(card_activation_date, TimestampType()),
        StructField(card_atm_limit, DoubleType()),
        StructField(card_bin, StringType()),
        StructField(card_change_date, TimestampType()),
        StructField(card_icc_card_ind, StringType()),
        StructField(card_pan_for_display, StringType()),
        StructField(card_pos_limit, DoubleType()),
        StructField(is_contact_chip_card, BooleanType()),
        StructField(is_contactless_chip_card, BooleanType()),
        StructField(is_contactless_mag_emu_card, BooleanType()),
        StructField(is_emv_card, BooleanType()),
        StructField(is_emv_cda_card, BooleanType()),
        StructField(is_mag_stripe_card, BooleanType()),
        StructField(pan_country_cd, StringType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        StructField(etl_custom_column, StringType()),
        StructField(tt_is_latest_data, BooleanType()),
    ])

class RISK_INDICATOR_RULES_INFO:
    # RISK_INDICATOR_RULES_INFO TABLE
    # please add column variable for new columns
    rule_category = 'RULE_CATEGORY'
    sub_category = 'SUB_CATEGORY'
    risk_indicator = 'RISK_INDICATOR'
    risk_indicator_condition = 'RISK_INDICATOR_CONDITION'
    risk_indicator_values = 'RISK_INDICATOR_VALUES'
    normalized_risk_weight = 'NORMALIZED_RISK_WEIGHT'
    risk_weight = 'RISK_WEIGHT'
    additional_filters = 'ADDITIONAL_FILTERS'
    tt_created_time = 'TT_CREATED_TIME'
    tt_updated_time = "TT_UPDATED_TIME"

    schema = StructType([
        StructField(rule_category, StringType()),
        StructField(sub_category, StringType()),
        StructField(risk_indicator, StringType()),
        StructField(risk_indicator_condition, StringType()),
        StructField(risk_indicator_values, StringType()),
        StructField(normalized_risk_weight, DoubleType()),
        StructField(risk_weight, DoubleType()),
        StructField(additional_filters, StringType()),
        StructField(tt_updated_time, TimestampType()),
    ])
    updated_schema = StructType([
        StructField(rule_category, StringType()),
        StructField(risk_indicator, StringType()),
        StructField(risk_indicator_condition, StringType()),
        StructField(risk_indicator_values, StringType()),
        StructField(normalized_risk_weight, DoubleType()),
        StructField(risk_weight, DoubleType()),
        StructField(additional_filters, StringType()),
        StructField(tt_created_time, TimestampType()),
    ])

class RISK_INDICATOR_RULES_MAPPINGS	:
    # RISK_INDICATOR_RULES_MAPPINGS	 TABLE
    # please add column variable for new columns
    rule_category = 'RULE_CATEGORY'
    sub_category = 'SUB_CATEGORY'
    risk_indicator = 'RISK_INDICATOR'
    tt_feature = 'TT_FEATURE'
    tt_updated_time = 'TT_UPDATED_TIME'
    tt_created_time = 'TT_CREATED_TIME'
    tt_is_deleted = 'TT_IS_DELETED'

    schema = StructType([
        StructField(rule_category, StringType()),
        StructField(sub_category, StringType()),
        StructField(risk_indicator, StringType()),
        StructField(tt_feature, StringType()),
        StructField(tt_updated_time, TimestampType()),
        StructField(tt_created_time, TimestampType()),
        StructField(tt_is_deleted, BooleanType()),
        ])