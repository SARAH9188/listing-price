from pyspark.sql.types import *


class CRS_CUSTOMERS:
    customer_key = 'CUSTOMER_KEY'
    customer_name = 'CUSTOMER_NAME'
    date_of_birth_incorporation = 'DATE_OF_BIRTH_INCORPORATION'
    customer_onboard_date = 'CUSTOMER_ONBOARD_DATE'
    customer_type = 'CUSTOMER_TYPE'
    customer_industry = 'CUSTOMER_INDUSTRY'
    customer_status = 'CUSTOMER_STATUS'
    kyc_review_date = 'KYC_REVIEW_DATE'
    division = 'DIVISION'
    type_of_business = 'TYPE_OF_BUSINESS'
    residential_address = 'RESIDENTIAL_ADDRESS'
    postal_code = 'POSTAL_CODE'
    contact_number = 'CONTACT_NUMBER'
    designation = 'DESIGNATION'
    gender = 'GENDER'
    nationality = 'NATIONALITY'
    id_number = 'ID_NUMBER'
    id_type = 'ID_TYPE'
    id_country = 'ID_COUNTRY'
    employer_name = 'EMPLOYER_NAME'
    pep_ind = 'PEP_IND'
    bank_staff_indicator = 'BANK_STAFF_INDICATOR'
    high_risk_party_info = 'HIGH_RISK_PARTY_INFO'
    annual_turnover = 'ANNUAL_TURNOVER'
    birth_incorporation_country = 'BIRTH_INCORPORATION_COUNTRY'
    residence_operation_country = 'RESIDENCE_OPERATION_COUNTRY'

    schema = StructType([
        StructField(customer_key, StringType()),
        StructField(customer_name, StringType()),
        StructField(date_of_birth_incorporation, DateType()),
        StructField(customer_onboard_date, DateType()),
        StructField(customer_type, StringType()),
        StructField(customer_industry, StringType()),
        StructField(customer_status, StringType()),
        StructField(kyc_review_date, DateType()),
        StructField(division, StringType()),
        StructField(type_of_business, StringType()),
        StructField(residential_address, StringType()),
        StructField(postal_code, StringType()),
        StructField(contact_number, StringType()),
        StructField(designation, StringType()),
        StructField(gender, StringType()),
        StructField(nationality, StringType()),
        StructField(id_number, StringType()),
        StructField(id_type, StringType()),
        StructField(id_country, StringType()),
        StructField(employer_name, StringType()),
        StructField(pep_ind, BooleanType()),
        StructField(bank_staff_indicator, BooleanType()),
        StructField(high_risk_party_info, StringType()),
        StructField(annual_turnover, StringType()),
        StructField(birth_incorporation_country, StringType()),
        StructField(residence_operation_country, StringType()),
    ])

    cols = [customer_key, customer_name, date_of_birth_incorporation, customer_onboard_date, customer_type,
            customer_industry, customer_status, kyc_review_date, division, type_of_business, residential_address,
            postal_code, contact_number, designation, gender, nationality, id_number, id_type, id_country,
            employer_name, pep_ind, bank_staff_indicator, high_risk_party_info, annual_turnover,
            birth_incorporation_country, residence_operation_country]


class INTERMMEDIATE_CRS_CUSTOMER_HISTORY:
    party_name ="PARTY_NAME"
    reference_date = "REFERENCE_DATE"
    employee_flag = "EMPLOYEE_FLAG"
    customer_key = 'CUSTOMER_KEY'
    crs_create_date = 'CRS_CREATE_DATE'
    ctb_cluster_id = 'CTB_CLUSTER_ID'
    ctb_model_id = 'CTB_MODEL_ID'
    overall_risk_score = 'OVERALL_RISK_SCORE'
    kyc_risk_score = 'KYC_RISK_SCORE'
    screening_risk_score = 'SCREENING_RISK_SCORE'
    network_risk_score = 'NETWORK_RISK_SCORE'
    transaction_behaviour_score = 'TRANSACTION_BEHAVIOUR_SCORE'
    risk_level = 'RISK_LEVEL'
    previous_risk_level = 'PREVIOUS_RISK_LEVEL'
    current_ml_suggested_risk_level = 'CURRENT_ML_SUGGESTED_RISK_LEVEL'
    current_risk_level = 'CURRENT_RISK_LEVEL'
    risk_shift_reason = 'RISK_SHIFT_REASON'
    segment_code = 'SEGMENT_CODE'
    product_name = 'PRODUCT_NAME'
    str_count = 'STR_COUNT'
    version_column = 'VERSION_COLUMN'
    nr_flag = 'NR_FLAG'
    segment_name = "SEGMENT_NAME"
    last_predicted_date = 'LAST_PREDICTED_DATE'


class CRS_CUSTOMER_HISTORY:
    customer_key = 'CUSTOMER_KEY'
    crs_create_date = 'CRS_CREATE_DATE'
    ctb_cluster_id = 'CTB_CLUSTER_ID'
    ctb_model_id = 'CTB_MODEL_ID'
    overall_risk_score = 'OVERALL_RISK_SCORE'
    kyc_risk_score = 'KYC_RISK_SCORE'
    screening_risk_score = 'SCREENING_RISK_SCORE'
    network_risk_score = 'NETWORK_RISK_SCORE'
    transaction_behaviour_score = 'TRANSACTION_BEHAVIOUR_SCORE'
    risk_level = 'RISK_LEVEL'
    previous_risk_level = 'PREVIOUS_RISK_LEVEL'
    current_ml_suggested_risk_level = 'CURRENT_ML_SUGGESTED_RISK_LEVEL'
    current_risk_level = 'CURRENT_RISK_LEVEL'
    risk_shift_reason = 'RISK_SHIFT_REASON'
    segment_code = 'SEGMENT_CODE'
    product_name = 'PRODUCT_NAME'
    str_count = 'STR_COUNT'
    version_column = 'VERSION_COLUMN'
    nr_flag = 'NR_FLAG'
    segment_name = "SEGMENT_NAME"
    last_predicted_date = 'LAST_PREDICTED_DATE'
    is_reviewed = 'IS_REVIEWED'

    schema = StructType([
        StructField(customer_key, StringType()),
        StructField(crs_create_date, DateType()),
        StructField(ctb_cluster_id, StringType()),
        StructField(ctb_model_id, StringType()),
        StructField(overall_risk_score, DoubleType()),
        StructField(kyc_risk_score, DoubleType()),
        StructField(screening_risk_score, DoubleType()),
        StructField(network_risk_score, DoubleType()),
        StructField(transaction_behaviour_score, DoubleType()),
        StructField(risk_level, StringType()),
        StructField(previous_risk_level, StringType()),
        StructField(current_ml_suggested_risk_level, StringType()),
        StructField(current_risk_level, StringType()),
        StructField(risk_shift_reason, StringType()),
        StructField(segment_code, StringType()),
        StructField(product_name, StringType()),
        StructField(str_count, IntegerType()),
        StructField(version_column, DoubleType()),
        StructField(nr_flag, BooleanType()),
        StructField(segment_name, StringType()),
        StructField(last_predicted_date, TimestampType()),
    ])

    cols = [customer_key, crs_create_date, ctb_cluster_id, ctb_model_id, overall_risk_score, kyc_risk_score,
            screening_risk_score, network_risk_score, transaction_behaviour_score, risk_level, previous_risk_level,
            current_ml_suggested_risk_level, current_risk_level, risk_shift_reason,
            segment_code, product_name, str_count, segment_name, last_predicted_date]


class CRS_MODEL_CLUSTER_DETAILS:
    model_id = 'MODEL_ID'
    cluster_id = 'CLUSTER_ID'
    crs_create_date = 'CRS_CREATE_DATE'
    cluster_size = 'CLUSTER_SIZE'
    severity_score = 'SEVERITY_SCORE'
    yield_rate = 'YIELD_RATE'
    cluster_segment_attributes = 'CLUSTER_SEGMENT_ATTRIBUTES'
    cluster_feature_stats = "CLUSTER_FEATURE_STATS"

    schema = StructType([
        StructField(model_id, StringType()),
        StructField(cluster_id, StringType()),
        StructField(crs_create_date, DateType()),
        StructField(cluster_size, IntegerType()),
        StructField(severity_score, DoubleType()),
        StructField(yield_rate, DoubleType()),
        StructField(cluster_segment_attributes, StringType()),
    ])

    cols = [model_id, cluster_id, crs_create_date, cluster_size, severity_score, yield_rate, cluster_segment_attributes, cluster_feature_stats]


class CRS_COMPONENT_RISK_SCORE:
    customer_key = 'CUSTOMER_KEY'
    component_create_timestamp = 'COMPONENT_CREATE_TIMESTAMP'
    crs_component = 'CRS_COMPONENT'
    risk_score = 'RISK_SCORE'
    risk_shift_reason = 'RISK_SHIFT_REASON'
    component_attributes = 'COMPONENT_ATTRIBUTES'

    schema = StructType([
        StructField(customer_key, StringType()),
        StructField(component_create_timestamp, TimestampType()),
        StructField(crs_component, StringType()),
        StructField(risk_score, DoubleType()),
        StructField(risk_shift_reason, StringType()),
        StructField(component_attributes, StringType()),
    ])

    cols = [customer_key, component_create_timestamp, crs_component, risk_score, risk_shift_reason,
            component_attributes]


class CRS_PROXY_INDICATOR_DECILE:
    alert_investigation_result= 'INVESTIGATION_RESULT'
    risk_bucket = "RISK_BUCKET"
    customer_key= "CUSTOMER_KEY"
    overall_risk_score = "OVERALL_RISK_SCORE"
    alert_created_date= "ALERT_DATE"

    schema = StructType([
        StructField(alert_investigation_result, IntegerType()),
        StructField(risk_bucket, StringType()),
        StructField(customer_key, StringType()),
        StructField(overall_risk_score, FloatType()),
        StructField(alert_created_date, TimestampType()),

    ])


class CRI_PROXY_PERFORMANCE_METRIC:
   precision_high_risk=  "PRECISION FOR HIGH RISK"
   misclassification_low_risk = "MISLCASSIFICATION IN LOW RISK"
   recall_high_risk = "RECALL FOR HIGH RISK"
   low_risk_reduction = "ALERT REDUCTION"

   schema = StructType([
       StructField(precision_high_risk, FloatType()),
       StructField(misclassification_low_risk, FloatType()),
       StructField(recall_high_risk, FloatType()),
       StructField(low_risk_reduction, FloatType()),
   ])


class CRS_THESHOLD:
    threshold_date = "THRESHOLD_DATE"
    high_risk_threshold= "HIGH_RISK_THRESHOLD"
    low_risk_threshold= "LOW_RISK_THRESHOLD"

    schema = StructType([
        StructField(threshold_date, TimestampType()),
        StructField(high_risk_threshold, FloatType()),
        StructField(low_risk_threshold, FloatType()),
    ])

class DECILE:
    ordered_bin = "ordered_bin"
    true_alert_sum = "true_alert_sum"
    non_true_alert_sum = "non_true_alert_sum"
    true_alert_predprob_avg = "true_alert_predprob_avg"
    true_alert_predprob_min = "true_alert_predprob_min"
    true_alert_predprob_max = "true_alert_predprob_max"
    true_alert_recall = "true_alert_recall"
    non_true_alert_reduction = "non_true_alert_reduction"
    cumsum_true_alert_recall = "cumsum_true_alert_recall"
    cumsum_non_true_alert_reduction = "cumsum_non_true_alert_reduction"

    schema = StructType([
        StructField(ordered_bin, DoubleType()),
        StructField(true_alert_sum, LongType()),
        StructField(non_true_alert_sum, LongType()),
        StructField(true_alert_predprob_avg, DoubleType()),
        StructField(true_alert_predprob_min, DoubleType()),
        StructField(true_alert_predprob_max, DoubleType()),
        StructField(true_alert_recall, DoubleType()),
        StructField(non_true_alert_reduction, DoubleType()),
        StructField(cumsum_true_alert_recall, DoubleType()),
        StructField(cumsum_non_true_alert_reduction, DoubleType())
    ])


class UI_TMALERTS:
    # post pipeline
    alert_id = 'ALERT_ID'
    alert_created_date = 'ALERT_DATE'
    reference_date = 'REFERENCE_DATE'
    primary_party_key = 'PRIMARY_PARTY_KEY'
    primary_party_name = 'PRIMARY_PARTY_NAME'
    alert_type = 'ALERT_TYPE'
    alert_status = 'ALERT_STATUS'
    rules = 'RULES'
    alert_level = 'ALERT_LEVEL'
    alert_score = 'ALERT_SCORE'
    segment_code = 'SEGMENT_CODE'
    segment_name = 'SEGMENT_NAME'
    account_key = 'ACCOUNT_ID'
    account_currency = 'ACCOUNT_CURRENCY'
    account_type = 'ACCOUNT_TYPE'
    account_open_date = 'ACCOUNT_OPEN_DATE'
    hits_score = 'HITS_SCORE'
    alert_age = 'ALERT_AGE'
    model_id = 'MODEL_ID'
    cluster_id = 'CLUSTER_ID'
    cluster_score = 'CLUSTER_SCORE'
    analyst_name = 'ANALYST_NAME'
    closure_date = 'CLOSURE_DATE'
    investigation_result = 'INVESTIGATION_RESULT'
    is_str = 'IS_STR'
    is_issue = 'IS_ISSUE'
    is_staff = 'IS_STAFF'
    prediction_date = 'PREDICTION_DATE'
    created_timestamp = 'CREATED_TIMESTAMP'
    updated_timestamp = 'UPDATED_TIMESTAMP'
    created_user = 'CREATED_USER'
    updated_user = 'UPDATED_USER'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'

    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(alert_created_date, TimestampType()),
        StructField(reference_date, TimestampType()),
        StructField(primary_party_key, StringType()),
        StructField(primary_party_name, StringType()),
        StructField(alert_type, IntegerType()),
        StructField(alert_status, IntegerType()),
        StructField(rules, StringType()),
        StructField(alert_level, StringType()),
        StructField(alert_score, DoubleType()),
        StructField(segment_code, StringType()),
        StructField(segment_name, StringType()),
        StructField(account_key, StringType()),
        StructField(account_currency, StringType()),
        StructField(account_type, StringType()),
        StructField(account_open_date, TimestampType()),
        StructField(hits_score, DoubleType()),
        StructField(alert_age, IntegerType()),
        StructField(model_id, IntegerType()),
        StructField(cluster_id, StringType()),
        StructField(cluster_score, DoubleType()),
        StructField(analyst_name, StringType()),
        StructField(closure_date, TimestampType()),
        StructField(investigation_result, StringType()),
        StructField(is_str, IntegerType()),
        StructField(is_issue, IntegerType()),
        StructField(is_staff, BooleanType()),
        StructField(prediction_date, TimestampType()),
        StructField(created_timestamp, TimestampType()),
        StructField(updated_timestamp, TimestampType()),
        StructField(created_user, StringType()),
        StructField(updated_user, StringType()),
        StructField(tt_updated_year_month, IntegerType()),
    ])
    table_cols = [
        alert_id, alert_created_date, reference_date, primary_party_key, primary_party_name, alert_type, alert_status,
        rules, alert_level, alert_score, segment_code, segment_name, account_key, account_currency, account_type,
        account_open_date, hits_score, alert_age, model_id, cluster_id, cluster_score, analyst_name, closure_date,
        investigation_result, is_str, is_issue, is_staff, prediction_date, created_timestamp, updated_timestamp,
        created_user, updated_user, tt_updated_year_month
    ]

class UI_CDDALERTS:
    # post pipeline
    alert_id = 'ALERT_ID'
    alert_created_date = 'ALERT_DATE'
    reference_date = 'REFERENCE_DATE'
    primary_party_key = 'PRIMARY_PARTY_KEY'
    primary_party_name = 'PRIMARY_PARTY_NAME'
    alert_type = 'ALERT_TYPE'
    alert_status = 'ALERT_STATUS'
    alert_reason = 'ALERT_REASON'
    rules = 'RULES'
    alert_level = 'ALERT_LEVEL'
    alert_score = 'ALERT_SCORE'
    segment_code = 'SEGMENT_CODE'
    segment_name = 'SEGMENT_NAME'
    account_currency = 'ACCOUNT_CURRENCY'
    account_type = 'ACCOUNT_TYPE'
    account_open_date = 'ACCOUNT_OPEN_DATE'
    hits_score = 'HITS_SCORE'
    alert_age = 'ALERT_AGE'
    model_id = 'MODEL_ID'
    cluster_id = 'CLUSTER_ID'
    cluster_score = 'CLUSTER_SCORE'
    analyst_name = 'ANALYST_NAME'
    closure_date = 'CLOSURE_DATE'
    investigation_result = 'INVESTIGATION_RESULT'
    is_str = 'IS_STR'
    is_issue = 'IS_ISSUE'
    is_staff = 'IS_STAFF'
    prediction_date = 'PREDICTION_DATE'
    created_timestamp = 'CREATED_TIMESTAMP'
    updated_timestamp = 'UPDATED_TIMESTAMP'
    created_user = 'CREATED_USER'
    updated_user = 'UPDATED_USER'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'

    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(alert_created_date, TimestampType()),
        StructField(reference_date, TimestampType()),
        StructField(primary_party_key, StringType()),
        StructField(primary_party_name, StringType()),
        StructField(alert_type, IntegerType()),
        StructField(alert_status, IntegerType()),
        StructField(alert_reason, StringType()),
        StructField(rules, StringType()),
        StructField(alert_level, StringType()),
        StructField(alert_score, DoubleType()),
        StructField(segment_code, StringType()),
        StructField(segment_name, StringType()),
        StructField(account_currency, StringType()),
        StructField(account_type, StringType()),
        StructField(account_open_date, TimestampType()),
        StructField(hits_score, DoubleType()),
        StructField(alert_age, IntegerType()),
        StructField(model_id, IntegerType()),
        StructField(cluster_id, StringType()),
        StructField(cluster_score, DoubleType()),
        StructField(analyst_name, StringType()),
        StructField(closure_date, TimestampType()),
        StructField(investigation_result, StringType()),
        StructField(is_str, IntegerType()),
        StructField(is_issue, IntegerType()),
        StructField(is_staff, BooleanType()),
        StructField(prediction_date, TimestampType()),
        StructField(created_timestamp, TimestampType()),
        StructField(updated_timestamp, TimestampType()),
        StructField(created_user, StringType()),
        StructField(updated_user, StringType()),
        StructField(tt_updated_year_month, IntegerType()),
    ])
    table_cols = [
        alert_id, alert_created_date, reference_date, primary_party_key, primary_party_name, alert_type, alert_status,
        rules, alert_level, alert_score, segment_code, segment_name, account_currency, account_type,
        account_open_date, hits_score, alert_age, model_id, cluster_id, cluster_score, analyst_name, closure_date,
        investigation_result, is_str, is_issue, is_staff, prediction_date, created_timestamp, updated_timestamp,
        created_user, updated_user, tt_updated_year_month
    ]


class UI_ALERTEDCUSTOMERS:
    # post pipeline
    alert_id = 'ALERT_ID'
    party_key = 'PARTY_KEY'
    party_name = 'PARTY_NAME'
    customer_type = 'CUSTOMER_TYPE'
    risk_rating = 'RISK_RATING'
    high_risk_party_info = 'HIGH_RISK_PARTY_INFO'
    age = 'AGE'
    occupation = 'OCCUPATION'
    gender = 'GENDER'
    segment_code = 'SEGMENT_CODE'
    segment = 'SEGMENT'
    dob_or_incorporation_date = 'DOB_OR_INCORPORATION_DATE'
    division = 'DIVISION'
    domicile_country = 'DOMICILE_COUNTRY'
    residential_address = 'RESIDENTIAL_ADDRESS'
    postal_code = 'POSTAL_CODE'
    is_staff = 'IS_STAFF'
    is_pep = 'IS_PEP'
    CUSTOMER_ACTIVITY_COL = 'ACTIVITY_DETAILS_TXN_BY_TYPE'
    CUSTOMER_ACTIVITY_ADDITIONAL_INFO_COL = 'ACTIVITY_DETAILS_TXN_ADDITIONAL_INFO'
    CONTACT_NUMBER = 'CONTACT_NUMBER'
    DOCUMENT_ID = 'DOCUMENT_ID'
    DOCUMENT_TYPE = 'DOCUMENT_TYPE'
    DOCUMENT_ISSUE_COUNTRY = 'DOCUMENT_ISSUE_COUNTRY'
    EMPLOYER_NAME = 'EMPLOYER_NAME'
    EMPLOYER_TYPE_OF_BUSINESS = 'EMPLOYER_TYPE_OF_BUSINESS'
    ANNUAL_TURNOVER = 'ANNUAL_TURNOVER'
    designation = 'DESIGNATION'
    residence_operation_country = 'RESIDENCE_OPERATION_COUNTRY'
    citizenship_country = 'CITIZENSHIP_COUNTRY'
    birth_incorporation_country = 'BIRTH_INCORPORATION_COUNTRY'
    created_timestamp = 'CREATED_TIMESTAMP'
    created_user = 'CREATED_USER'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'
    table_cols = [
        alert_id, party_key, party_name, customer_type, risk_rating, high_risk_party_info, age, occupation, gender,
        segment_code, segment, dob_or_incorporation_date, division, domicile_country, residential_address, postal_code,
        is_staff, is_pep, CUSTOMER_ACTIVITY_COL, CUSTOMER_ACTIVITY_ADDITIONAL_INFO_COL, CONTACT_NUMBER, DOCUMENT_ID,
        DOCUMENT_TYPE, DOCUMENT_ISSUE_COUNTRY, EMPLOYER_NAME, EMPLOYER_TYPE_OF_BUSINESS, ANNUAL_TURNOVER, designation,
        residence_operation_country, citizenship_country, birth_incorporation_country, created_timestamp, created_user,
        tt_updated_year_month
    ]


class UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS:
    # post pipeline
    alert_id = 'ALERT_ID'
    primary_party_key = 'PRIMARY_PARTY_KEY'
    linked_party_key = 'LINKED_PARTY_KEY'
    party_name = 'PARTY_NAME'
    risk_rating = 'RISK_RATING'
    occupation = 'OCCUPATION'
    segment_code = 'SEGMENT_CODE'
    segment = 'SEGMENT'
    nationality = 'NATIONALITY'
    dob_or_incorporation_date = 'DOB_OR_INCORPORATION_DATE'
    relation_type = 'RELATION_TYPE'
    relation_description = 'RELATION_DESCRIPTION'
    shareholder_percentage = 'SHAREHOLDER_PERCENTAGE'
    is_staff = 'IS_STAFF'
    created_timestamp = 'CREATED_TIMESTAMP'
    created_user = 'CREATED_USER'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'
    table_cols = [
        alert_id, primary_party_key, linked_party_key, party_name, risk_rating, occupation, segment, segment_code,
        nationality, dob_or_incorporation_date, relation_type, relation_description, shareholder_percentage, is_staff,
        created_timestamp, created_user, tt_updated_year_month
    ]


class UI_ALERTEDCUSTOMERSTOACCOUNTS:
    # post pipeline
    alert_id = 'ALERT_ID'
    account_id = 'ACCOUNT_ID'
    product_id = 'PRODUCT_ID'
    account_key = 'ACCOUNT_KEY'
    c2a_relationship = 'C2A_RELATIONSHIP'
    product = 'PRODUCT'
    open_date = 'OPEN_DATE'
    balance = 'BALANCE'
    credit_limit = 'CREDIT_LIMIT'
    card_number = 'CARD_NUMBER'
    account_name = 'ACCOUNT_NAME'
    account_closed_date = 'ACCOUNT_CLOSED_DATE'
    account_status = 'ACCOUNT_STATUS'
    account_currency = 'ACCOUNT_CURRENCY'
    account_dr_avg_balance = 'ACCOUNT_DR_AVG_BALANCE'
    account_cr_avg_balance = 'ACCOUNT_CR_AVG_BALANCE'
    fd_receipt_number = 'FD_RECEIPT_NUMBER'
    fd_original_amount = 'FD_ORIGINAL_AMOUNT'
    fd_placement_date = 'FD_PLACEMENT_DATE'
    fd_maturity_date = 'FD_MATURITY_DATE'
    fd_withdrawal_date = 'FD_WITHDRAWAL_DATE'
    outstanding_loan_balance = 'OUTSTANDING_LOAN_BALANCE'
    loan_amount = 'LOAN_AMOUNT'
    loan_installment_amount = 'LOAN_INSTALLMENT_AMOUNT'
    loan_group = 'LOAN_GROUP'
    loan_type = 'LOAN_TYPE'
    loan_purpose_code = 'LOAN_PURPOSE_CODE'
    loan_gl_group = 'LOAN_GL_GROUP'
    loan_disbursement_date = 'LOAN_DISBURSEMENT_DATE'
    loan_maturity_date = 'LOAN_MATURITY_DATE'
    total_repaid_amount = 'TOTAL_REPAID_AMOUNT'
    number_of_repayments = 'NUMBER_OF_REPAYMENTS'
    created_timestamp = 'CREATED_TIMESTAMP'
    created_user = 'CREATED_USER'
    updated_timestamp = 'UPDATED_TIMESTAMP'
    updated_user = 'UPDATED_USER'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'
    table_cols = [
        alert_id, account_id, product_id, account_key, c2a_relationship, product, open_date, balance, credit_limit,
        card_number, account_name, account_closed_date, account_status, account_currency, account_dr_avg_balance,
        account_cr_avg_balance, fd_receipt_number, fd_original_amount, fd_placement_date, fd_maturity_date,
        fd_withdrawal_date, outstanding_loan_balance, loan_amount, loan_installment_amount, loan_group, loan_type,
        loan_purpose_code, loan_gl_group, loan_disbursement_date, loan_maturity_date, total_repaid_amount,
        number_of_repayments, created_timestamp, created_user, updated_timestamp, updated_user, tt_updated_year_month
    ]


class UI_TRANSACTIONS:
    # post pipeline
    id = 'ID'
    party_key = 'PARTY_KEY'
    transaction_date = 'TRANSACTION_DATE'
    product_code = 'PRODUCT_CODE'
    account_currency = 'ACCOUNT_CURRENCY'
    amount = 'AMOUNT'
    transaction_currency = 'TRANSACTION_CURRENCY'
    transaction_currency_amount = 'TRANSACTION_CURRENCY_AMOUNT'
    local_currency_equivalent = 'LOCAL_CURRENCY_EQUIVALENT'
    is_credit = 'IS_CREDIT'
    account_id = 'ACCOUNT_ID'
    transaction_type_code = 'TRANSACTION_TYPE_CODE'
    transaction_type_description = 'TRANSACTION_TYPE_DESCRIPTION'
    beneficiary = 'BENEFICIARY'
    originator_address = 'ORIGINATOR_ADDRESS'
    originator_bank_country_code = 'ORIGINATOR_BANK_COUNTRY_CODE'
    beneficiary_address = 'BENEFICIARY_ADDRESS'
    beneficiary_bank_country_code = 'BENEFICIARY_BANK_COUNTRY_CODE'
    remittance_country = 'REMITTANCE_COUNTRY'
    remittance_payment_details = 'REMITTANCE_PAYMENT_DETAILS'
    cheque_no = 'CHEQUE_NO'
    teller_id = 'TELLER_ID'
    your_reference = 'YOUR_REFERENCE'
    our_reference = 'OUR_REFERENCE'
    bank_info = 'BANK_INFO'
    country_of_transaction = 'COUNTRY_OF_TRANSACTION'
    mcc = 'MCC'
    merchant_name = 'MERCHANT_NAME'
    originator = 'ORIGINATOR'
    atm_id = 'ATM_ID'
    atm_location = 'ATM_LOCATION'
    counter_party = 'COUNTER_PARTY'
    swift_msg_type = 'SWIFT_MSG_TYPE'
    swift_msg_info = 'SWIFT_MSG_INFO'
    td_number = 'TD_NUMBER'
    tranche_no = 'TRANCHE_NO'
    credit_card_number = 'CREDIT_CARD_NUMBER'
    country_code_high_risk= "HIGH_RISK_COUNTRY_FLAG"
    table_cols = [
        id, party_key, transaction_date, product_code, account_currency, amount, transaction_currency,
        transaction_currency_amount, local_currency_equivalent, is_credit, account_id, transaction_type_code,
        transaction_type_description, beneficiary, originator_address, originator_bank_country_code,
        beneficiary_address, beneficiary_bank_country_code, remittance_country, remittance_payment_details, cheque_no,
        teller_id, your_reference, our_reference, bank_info, country_of_transaction, mcc, merchant_name, originator,
        atm_id, atm_location, counter_party, swift_msg_type, swift_msg_info, td_number, tranche_no, credit_card_number,
        country_code_high_risk
    ]


# Cluster and risk indicator related tables
class RiskIndicator:
    # post pipeline
    risk_indicator_id = 'RISK_INDICATOR_ID'
    risk_indicator_name = 'RISK_INDICATOR_NAME'
    comparison_x_axis_name = 'COMPARISON_X_AXIS_NAME'
    comparison_y_axis_name = 'COMPARISON_Y_AXIS_NAME'
    deviation_x_axis_name = 'DEVIATION_X_AXIS_NAME'
    deviation_y_axis_name = 'DEVIATION_Y_AXIS_NAME'


class ClusterRiskIndicatorStats:
    # post pipeline
    model_id = 'MODEL_ID'
    cluster_id = 'CLUSTER_ID'
    risk_indicator_id = 'RISK_INDICATOR_ID'
    feature_type = 'FEATURE_TYPE'
    distribution_range = 'DISTRIBUTION_RANGE'
    distribution_density = 'DISTRIBUTION_DENSITY'
    mean = 'MEAN'
    standard_deviation = 'STANDARD_DEVIATION'
    cluster_comparison = 'CLUSTER_COMPARISON'
    attributes = 'ATTRIBUTES'


class CustomerCluster:
    # post pipeline
    cluster_id = 'CLUSTER_ID'
    model_id = 'MODEL_ID'
    cluster_name = 'CLUSTER_NAME'
    cluster_size = 'CLUSTER_SIZE'
    historical_alert_count = 'HISTORICAL_ALERT_COUNT'
    historical_str_count = 'HISTORICAL_STR_COUNT'
    attributes = 'ATTRIBUTES'


class ClusterModel:
    # post pipeline
    model_id = 'MODEL_ID'
    training_date = 'TRAINING_DATE'
    cluster_count = 'CLUSTER_COUNT'
    popution_size = 'POPULATION_SIZE'


class CustomerClusterRiskIndicatorStats:
    # post pipeline
    model_id = 'MODEL_ID'
    alert_id = 'ALERT_ID'
    customer_id = 'PARTY_KEY'
    cluster_id = 'CLUSTER_ID'
    risk_indicator_id = 'RISK_INDICATOR_ID'
    ri_value = 'RI_VALUE'
    percentile = 'PERCENTILE'
    profile_mean = 'PROFILE_MEAN'
    cluster_deviation = 'CLUSTER_DEVIATION'
    profile_deviation = 'PROFILE_DEVIATION'
    time_range = 'TIME_RANGE'
    amount_over_period = 'AMOUNT_OVER_PERIOD'
    attributes = 'ATTRIBUTES'
    created_timestamp = 'CREATED_TIMESTAMP'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'
    schema = StructType([StructField(model_id, LongType(), True), StructField(alert_id, StringType(), False),
                         StructField(customer_id, StringType(), True), StructField(cluster_id, StringType(), True),
                         StructField(risk_indicator_id, IntegerType(), True), StructField(ri_value, DoubleType(), True),
                         StructField(percentile, DoubleType(), True), StructField(profile_mean, DoubleType(), True),
                         StructField(cluster_deviation, DoubleType(), True),
                         StructField(profile_deviation, DoubleType(), True),
                         StructField(amount_over_period, StringType(), True),
                         StructField(attributes, StringType(), True),
                         StructField(created_timestamp, TimestampType(), True),
                         StructField(tt_updated_year_month, IntegerType(), True)])


class CLUSTER_SCORE:
    # intermidiate table
    cluster_id = 'Combined_ID'
    cluster_score = 'CLUSTER_SCORE'


class MODEL_TABLE:
    # model table
    model_id = 'MODEL_ID'


class TM_MISSING_ALERT:
    # tm_missing_alert
    alert_id = 'ALERT_ID'
    alert_date = 'ALERT_DATE'
    error_code = 'ERROR_CODE'
    log_date = 'LOG_DATE'
    created_timestamp = 'CREATED_TIMESTAMP'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'
    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(alert_date, TimestampType()),
        StructField(error_code, StringType()),
        StructField(log_date, DateType()),
        StructField(created_timestamp, TimestampType()),
        StructField(tt_updated_year_month, IntegerType())
    ])

class CDD_MISSING_ALERT:
    # cdd_missing_alert
    alert_id = 'ALERT_ID'
    alert_date = 'ALERT_DATE'
    error_code = 'ERROR_CODE'
    log_date = 'LOG_DATE'
    created_timestamp = 'CREATED_TIMESTAMP'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'
    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(alert_date, TimestampType()),
        StructField(error_code, StringType()),
        StructField(log_date, DateType()),
        StructField(created_timestamp, TimestampType()),
        StructField(tt_updated_year_month, IntegerType())
    ])

class MODEL_OUTPUT:
    # post pipeline
    alert_id = 'ALERT_ID'
    feature_name = "FEATURE_NAME"
    feature_contribution = "FEATURE_CONTRIBUTION"
    typology_name = "TYPOLOGY_NAME"
    typology_contribution = "TYPOLOGY_CONTRIBUTION"
    group_name = "GROUP_NAME"
    group_contribution = "GROUP_CONTRIBUTION"
    feature_value = 'FEATURE_VALUE'
    feature_component_breakdown = 'FEATURE_COMPONENT_BREAKDOWN'
    created_timestamp = 'CREATED_TIMESTAMP'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'

    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(feature_name, StringType()),
        StructField(feature_contribution, DoubleType()),
        StructField(typology_name, StringType()),
        StructField(typology_contribution, DoubleType()),
        StructField(group_name, StringType()),
        StructField(group_contribution, DoubleType()),
        StructField(feature_value, StringType()),
        StructField(feature_component_breakdown, StringType()),
        StructField(created_timestamp, TimestampType()),
        StructField(tt_updated_year_month, IntegerType())
    ])
    table_cols = [
        alert_id, feature_name, feature_contribution, typology_name, typology_contribution, group_name,
        group_contribution, feature_value, feature_component_breakdown, created_timestamp, tt_updated_year_month
    ]


class CTB_CUSTOMER_CLUSTER_RI_STATS:
    customer_id = 'CUSTOMER_KEY'
    risk_indicator_id = 'RISK_INDICATOR_ID'
    model_id = 'MODEL_ID'
    cluster_id = 'CLUSTER_ID'
    ri_value = 'RI_VALUE'
    percentile = 'PERCENTILE'
    profile_mean = 'PROFILE_MEAN'
    cluster_deviation = 'CLUSTER_DEVIATION'
    profile_deviation = 'PROFILE_DEVIATION'
    amount_over_period = 'AMOUNT_OVER_PERIOD'
    attributes = 'ATTRIBUTES'

    schema = StructType([
        StructField(customer_id, StringType(), True),
        StructField(risk_indicator_id, IntegerType(), True),
        StructField(model_id, LongType(), True),
        StructField(cluster_id, StringType(), True),
        StructField(ri_value, DoubleType(), True),
        StructField(percentile, DoubleType(), True),
        StructField(profile_mean, DoubleType(), True),
        StructField(cluster_deviation, DoubleType(), True),
        StructField(profile_deviation, DoubleType(), True),
        StructField(amount_over_period, StringType(), True),
        StructField(attributes, StringType(), True)])


class CTB_EXPLAINABILITY:
    customer_key = 'CUSTOMER_KEY'
    group_name = 'GROUP_NAME'
    typology_name = 'TYPOLOGY_NAME'
    feature_name = 'FEATURE_NAME'
    tt_version = 'TT_VERSION'
    group_contribution = 'GROUP_CONTRIBUTION'
    typology_contribution = 'TYPOLOGY_CONTRIBUTION'
    feature_contribution = 'FEATURE_CONTRIBUTION'
    feature_value = 'FEATURE_VALUE'
    feature_component_breakdown = 'FEATURE_COMPONENT_BREAKDOWN'
    explainability_category = 'EXPLAINABILITY_CATEGORY'
    alert_id = 'ALERT_ID'

    schema = StructType([
        StructField(customer_key, StringType(), True),
        StructField(group_name, StringType(), True),
        StructField(typology_name, StringType(), True),
        StructField(feature_name, StringType(), True),
        StructField(tt_version, IntegerType(), True),
        StructField(group_contribution, DoubleType(), True),
        StructField(typology_contribution, DoubleType(), True),
        StructField(feature_contribution, DoubleType(), True),
        StructField(feature_value, StringType(), True),
        StructField(feature_component_breakdown, StringType(), True),
        StructField(explainability_category, StringType(), True),
        StructField(alert_id, StringType(), True)
    ])


class CODES:
    # CODES TABLE

    code = 'CODE'
    code_type = 'CODE_TYPE'
    tt_module = 'TT_MODULE'
    code_description = 'DESCRIPTION'

    schema = StructType([
        StructField(code, StringType()),
        StructField(code_type, StringType()),
        StructField(tt_module, StringType()),
        StructField(code_description, StringType()),
    ])



class CRS_EXPLAINABILITY_STATS:
    # CRS_EXPLAINABILITY_STATS  TABLE
    customer_key = 'CUSTOMER_KEY'
    explainability_category = 'EXPLAINABILITY_CATEGORY'
    risk_level = 'RISK_LEVEL'
    tt_version = 'TT_VERSION'
    alert_id = 'ALERT_ID'

    schema = StructType([
        StructField(customer_key, StringType(), True),
        StructField(explainability_category, StringType(), True),
        StructField(risk_level, IntegerType()),
        StructField(tt_version, IntegerType(), True)])


class VERSIONS_TABLE:
    pipeline_id= 'PIPELINE_ID'
    mode_of_run = "MODE_OF_RUN"
    latest_version = "LATEST_VERSION"
    previous_version= "PREVIOUS_VERSION"
    updated_timestamp = "UPDATED_TIMESTAMP"

    schema = StructType([
        StructField(pipeline_id, IntegerType()),
        StructField(mode_of_run, StringType()),
        StructField(latest_version, DoubleType()),
        StructField(previous_version, DoubleType()),
        StructField(updated_timestamp, TimestampType()),
    ])

class UI_UNIFIEDALERTS:
    # post pipeline
    alert_id = 'ALERT_ID'
    alert_created_date = 'ALERT_DATE'
    reference_date = 'REFERENCE_DATE'
    primary_party_key = 'PRIMARY_PARTY_KEY'
    primary_party_name = 'PRIMARY_PARTY_NAME'
    alert_type = 'ALERT_TYPE'
    alert_status = 'ALERT_STATUS'
    alert_reason = 'ALERT_REASON'
    rules = 'RULES'
    alert_level = 'ALERT_LEVEL'
    alert_score = 'ALERT_SCORE'
    segment_code = 'SEGMENT_CODE'
    segment_name = 'SEGMENT_NAME'
    account_key = 'ACCOUNT_ID'
    account_currency = 'ACCOUNT_CURRENCY'
    account_type = 'ACCOUNT_TYPE'
    account_open_date = 'ACCOUNT_OPEN_DATE'
    hits_score = 'HITS_SCORE'
    alert_age = 'ALERT_AGE'
    model_id = 'MODEL_ID'
    cluster_id = 'CLUSTER_ID'
    cluster_score = 'CLUSTER_SCORE'
    analyst_name = 'ANALYST_NAME'
    closure_date = 'CLOSURE_DATE'
    investigation_result = 'INVESTIGATION_RESULT'
    is_str = 'IS_STR'
    is_issue = 'IS_ISSUE'
    is_staff = 'IS_STAFF'
    prediction_date = 'PREDICTION_DATE'
    created_timestamp = 'CREATED_TIMESTAMP'
    updated_timestamp = 'UPDATED_TIMESTAMP'
    created_user = 'CREATED_USER'
    updated_user = 'UPDATED_USER'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'

    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(alert_created_date, TimestampType()),
        StructField(reference_date, TimestampType()),
        StructField(primary_party_key, StringType()),
        StructField(primary_party_name, StringType()),
        StructField(alert_type, IntegerType()),
        StructField(alert_status, IntegerType()),
        StructField(alert_reason, StringType()),
        StructField(rules, StringType()),
        StructField(alert_level, StringType()),
        StructField(alert_score, DoubleType()),
        StructField(segment_code, StringType()),
        StructField(segment_name, StringType()),
        StructField(account_key, StringType()),
        StructField(account_currency, StringType()),
        StructField(account_type, StringType()),
        StructField(account_open_date, TimestampType()),
        StructField(hits_score, DoubleType()),
        StructField(alert_age, IntegerType()),
        StructField(model_id, IntegerType()),
        StructField(cluster_id, StringType()),
        StructField(cluster_score, DoubleType()),
        StructField(analyst_name, StringType()),
        StructField(closure_date, TimestampType()),
        StructField(investigation_result, StringType()),
        StructField(is_str, IntegerType()),
        StructField(is_issue, IntegerType()),
        StructField(is_staff, BooleanType()),
        StructField(prediction_date, TimestampType()),
        StructField(created_timestamp, TimestampType()),
        StructField(updated_timestamp, TimestampType()),
        StructField(created_user, StringType()),
        StructField(updated_user, StringType()),
        StructField(tt_updated_year_month, IntegerType()),
    ])
    table_cols = [
        alert_id, alert_created_date, reference_date, primary_party_key, primary_party_name, alert_type, alert_status,
        rules, alert_level, alert_score, segment_code, segment_name, account_key, account_currency, account_type,
        account_open_date, hits_score, alert_age, model_id, cluster_id, cluster_score, analyst_name, closure_date,
        investigation_result, is_str, is_issue, is_staff, prediction_date, created_timestamp, updated_timestamp,
        created_user, updated_user, tt_updated_year_month
    ]


class TRACKER_INFO:
    first_run = 'FIRST_RUN'
    rule_based_flow = "RULE_BASED_FLOW"
    clustering_flow = "CLUSTERING_FLOW"
    supervised_flow = "SUPERVISED_FLOW"
    first_pred_completed = "FIRST_PRED_COMPLETED"
    updated_timestamp = "UPDATED_TIMESTAMP"

    schema = StructType([
        StructField(first_run, BooleanType()),
        StructField(rule_based_flow, BooleanType()),
        StructField(clustering_flow, BooleanType()),
        StructField(supervised_flow, BooleanType()),
        StructField(first_pred_completed, BooleanType()),
        StructField(updated_timestamp, TimestampType()),
    ])


class CMALERTS:

    ID = "ID"
    SOURCE_ID = "SOURCE_ID"
    TIMESTAMP = "TIMESTAMP"
    SCORE = "SCORE"
    LEVEL = "LEVEL"
    CUSTOMER_ID = "CUSTOMER_ID"
    CUSTOMER_NAME = "CUSTOMER_NAME"
    PRIMARY_KEY = "PRIMARY_KEY"
    ACCOUNT_CURRENCY = "ACCOUNT_CURRENCY"
    ACCOUNT_NUMBER = "ACCOUNT_NUMBER"
    cols = [ID, SOURCE_ID, TIMESTAMP, SCORE, LEVEL, CUSTOMER_ID, CUSTOMER_NAME, PRIMARY_KEY, ACCOUNT_CURRENCY,
            ACCOUNT_NUMBER]


class UNIFIED_ALERTS:
    ALERT_ID = "ALERT_ID"
    ALERT_SOURCE_ID = "ALERT_SOURCE_ID"  # differnt types of alerts based on that (********)
    ALERT_DATE = "ALERT_DATE"
    REFERENCE_DATE = "REFERENCE_DATE"
    PRIMARY_PARTY_KEY = "PRIMARY_PARTY_KEY"
    PRIMARY_PARTY_NAME = "PRIMARY_PARTY_NAME"
    ALERT_STATUS = "ALERT_STATUS"  # reconfirm (********)
    RULES = "RULES"
    ALERT_LEVEL = "ALERT_LEVEL"  # always high(********)
    ALERT_SCORE = "ALERT_SCORE"
    SEGMENT_NAME = "SEGMENT_NAME"
    SEGMENT_CODE = "SEGMENT_CODE"
    ACCOUNT_ID = "ACCOUNT_ID"  # Null
    ACCOUNT_CURRENCY = "ACCOUNT_CURRENCY"  # Null
    ACCOUNT_TYPE = "ACCOUNT_TYPE"  # Null
    ACCOUNT_OPEN_DATE = "ACCOUNT_OPEN_DATE"  # Null
    HITS_SCORE = "HITS_SCORE"  # Null
    ALERT_AGE = "ALERT_AGE"  # fill woth 0
    CLUSTER_ID = "CLUSTER_ID"  # CTB_CLUSTER_ID
    MODEL_ID = "MODEL_ID"  # CTB_MODEL_ID
    CLUSTER_SCORE = "CLUSTER_SCORE"  # None
    ANALYST_NAME = "ANALYST_NAME"  # Null
    CLOSURE_DATE = "CLOSURE_DATE"  # Null
    INVESTIGATION_RESULT = "INVESTIGATION_RESULT"  # Null
    IS_STR = "IS_STR"  # Null
    IS_ISSUE = "IS_ISSUE"  # Null
    IS_STAFF = "IS_STAFF"  # employee_flag
    INVESTIGATOR_NOTE = "INVESTIGATOR_NOTE"  # Null
    SUPERVISOR_NOTE = "SUPERVISOR_NOTE"  # Null
    NEXT_ACTIONS = "NEXT_ACTIONS"  # Null
    REOPEN_DATE = "REOPEN_DATE"  # Null
    ATTACHMENT_ID = "ATTACHMENT_ID"  # Null
    ATTACHMENT_NAME = "ATTACHMENT_NAME"  # Null
    PREDICTION_DATE = "PREDICTION_DATE"  # ALERT_DATE
    IS_WHITELISTED = "IS_WHITELISTED"  # Null
    PARTY_TYPE = "PARTY_TYPE"  # ENTITY_TYPE
    PARTY_ORG_UNIT = "PARTY_ORG_UNIT"  # To be added in the connectors for customers(********) CUSTOMER_ORGUNIT_CODE
    PARTY_RACE = "PARTY_RACE"  # RACE
    PARTY_DOB = "PARTY_DOB"  # DATE_OF_BIRTH_OR_INCORPORATION
    PARTY_ADDRESS = "PARTY_ADDRESS"  # RESIDENTIAL_ADDRESS
    PARTY_ID_TYPE = "PARTY_ID_TYPE"  # CUSTOMER_ID_TYPE
    PARTY_ID_NO = "PARTY_ID_NO"  # CUSTOMER_ID_NO
    PARTY_ID_COUNTRY = "PARTY_ID_COUNTRY"  # CUSTOMER_ID_COUNTRY
    PARTY_ALIASES = "PARTY_ALIASES"  # To be added in the connectors for customers(********) CUSTOMER_ALIASES
    PARTY_GENDER = "PARTY_GENDER"  # GENDER
    PARTY_AGE = "PARTY_AGE"  # To be calculated from the dob(********)
    PARTY_NATIONALITY = "PARTY_NATIONALITY"  # CITIZENSHIP_COUNTRY
    PARTY_BIRTH_COUNTRY = "PARTY_BIRTH_COUNTRY"  # BIRTH_INCORPORATION_COUNTRY
    PARTY_RESIDENCE_COUNTRY = "PARTY_RESIDENCE_COUNTRY"  # RESIDENCE_OPERATION_COUNTRY
    PARTY_BUSINESS_TYPE = "PARTY_BUSINESS_TYPE"  # BUSINESS_TYPE
    PARTY_CRS_RISK_SCORE = "PARTY_CRS_RISK_SCORE"  # Overall_risk_score
    HITS_SUMMARY = "HITS_SUMMARY"  # Null
    CREATED_TIMESTAMP = "CREATED_TIMESTAMP"  # ALERT_DATE
    UPDATED_TIMESTAMP = "UPDATED_TIMESTAMP"  # ALERT_DATE
    CREATED_USER = "CREATED_USER"  # Nullreconfirm(********)
    UPDATED_USER = "UPDATED_USER"  # Nullreconfirm(********)
    RUN_INSTANCE_EXECUTION_ID = "RUN_INSTANCE_EXECUTION_ID"  # Null
    TRIGGERED_TYPOLOGIES = "TRIGGERED_TYPOLOGIES"  # Null
    ACTIONABLE_INSIGHTS = "ACTIONABLE_INSIGHTS"  # Null
    PIPELINE_ID = "PIPELINE_ID"
    PIPELINE_INSTANCE_ID = "PIPELINE_INSTANCE_ID"
    SCENARIO_NAME = "SCENARIO_NAME"  # reconfirm (********)
    SCENARIO_ID = "SCENARIO_ID"  # reconfirm (********)
    ALERT_QUALITY = "ALERT_QUALITY"  # reconfirm (********)
    cols = [ALERT_ID, ALERT_SOURCE_ID, ALERT_DATE, REFERENCE_DATE, PRIMARY_PARTY_KEY, PRIMARY_PARTY_NAME,
            ALERT_STATUS, RULES, ALERT_LEVEL, ALERT_SCORE, SEGMENT_NAME, SEGMENT_CODE, ACCOUNT_ID, ACCOUNT_CURRENCY,
            ACCOUNT_TYPE, ACCOUNT_OPEN_DATE, HITS_SCORE, ALERT_AGE, CLUSTER_ID, MODEL_ID, CLUSTER_SCORE,
            INVESTIGATION_RESULT, IS_STR, IS_ISSUE, IS_STAFF, INVESTIGATOR_NOTE, SUPERVISOR_NOTE, NEXT_ACTIONS,
            ATTACHMENT_ID, ATTACHMENT_NAME, PREDICTION_DATE, IS_WHITELISTED, PARTY_TYPE,PARTY_ORG_UNIT, PARTY_RACE,
            PARTY_DOB, PARTY_ADDRESS, PARTY_ID_TYPE, PARTY_ID_NO, PARTY_ID_COUNTRY, PARTY_ALIASES, PARTY_GENDER,
            PARTY_AGE, PARTY_NATIONALITY, PARTY_BIRTH_COUNTRY, PARTY_RESIDENCE_COUNTRY, PARTY_BUSINESS_TYPE,
            PARTY_CRS_RISK_SCORE, HITS_SUMMARY, CREATED_TIMESTAMP,  UPDATED_TIMESTAMP, CREATED_USER, UPDATED_USER,
            RUN_INSTANCE_EXECUTION_ID, TRIGGERED_TYPOLOGIES, ACTIONABLE_INSIGHTS, PIPELINE_ID, PIPELINE_INSTANCE_ID,
            SCENARIO_NAME, SCENARIO_ID, ALERT_QUALITY]

class CRS_THRESHOLDS:
    # CRS_THRESHOLDS TABLE
    low_risk_threshold = 'LOW_RISK_THRESHOLD'
    high_risk_threshold = 'HIGH_RISK_THRESHOLD'
    model_id = 'MODEL_ID'
    threshold_type = 'THRESHOLD_TYPE'
    threshold_date= 'THRESHOLD_DATE'

    schema = StructType([
        StructField(low_risk_threshold, DoubleType()),
        StructField(high_risk_threshold, DoubleType()),
        StructField(model_id, LongType()),
        StructField(threshold_type, StringType()),
        StructField(threshold_date, TimestampType()),
    ])