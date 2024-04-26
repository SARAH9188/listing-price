from pyspark.sql.types import *


class ANOMALY_OUTPUT:
    # intermidiate table
    anomaly_id = 'ANOMALY_ID'
    party_key = 'PARTY_KEY'
    anomaly_date = 'ANOMALY_DATE'
    create_date = 'CREATE_DATE'
    cluster_id = 'Combined_ID'
    anomaly_score = 'ANOMALY_SCORE'
    ensemble_score = 'ENSEMBLE_SCORE'
    ensemble_model = 'ENSEMBLE_MODEL'
    party_age = 'PARTY_AGE'
    is_anomaly = 'FINAL_PREDICT'
    alert_investigation_result = 'ALERT_INVESTIGATION_RESULT_CUSTOMER'
    high_risk_incoming_90day_amt = 'high-risk-incoming_90DAY_AMT_CUSTOMER'
    high_risk_outgoing_90day_amt = 'high-risk-outgoing_90DAY_AMT_CUSTOMER'
    ratio_incoming_all_outgoing_all_90day_amt = 'ratio_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER'
    net_incoming_all_outgoing_all_90day_amt = 'net_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER'
    incoming_local_fund_transfer_90day_amt = 'incoming-local-fund-transfer_90DAY_AMT_CUSTOMER'
    outgoing_local_fund_transfer_90day_amt = 'outgoing-local-fund-transfer_90DAY_AMT_CUSTOMER'
    outgoing_fund_transfer_high_risk_country_360day_amt = 'outgoing-fund-transfer_360DAY_TO-HIGH-RISK-COUNTRY_AMT_CUSTOMER'
    cdm_90day_amt = 'CDM-cash-deposit_90DAY_AMT_CUSTOMER'
    incoming_overseas_fund_transfer_360day_amt = 'incoming-overseas-fund-transfer_360DAY_AMT_CUSTOMER'
    outgoing_overseas_fund_transfer_360day_amt = 'outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER'
    incoming_all_90day_amt = 'incoming-all_90DAY_AMT_CUSTOMER'
    outgoing_all_90day_amt = 'outgoing-all_90DAY_AMT_CUSTOMER'
    incoming_all_3month_monthly_avg = 'incoming-all_3MONTH_MONTHLY_AVG_CUSTOMER'
    outgoing_all_3month_monthly_avg = 'outgoing-all_3MONTH_MONTHLY_AVG_CUSTOMER'
    incoming_all_90day_max = 'incoming-all_90DAY_MAX_CUSTOMER'
    outgoing_all_90day_max = 'outgoing-all_90DAY_MAX_CUSTOMER'
    incoming_all_180day_amt = 'incoming-all_180DAY_AMT_CUSTOMER'
    outgoing_all_180day_amt = 'outgoing-all_180DAY_AMT_CUSTOMER'
    incoming_all_6month_monthly_avg = 'incoming-all_6MONTH_MONTHLY_AVG_CUSTOMER'
    outgoing_all_6month_monthly_avg = 'outgoing-all_6MONTH_MONTHLY_AVG_CUSTOMER'
    incoming_all_180day_max = 'incoming-all_180DAY_MAX_CUSTOMER'
    outgoing_all_180day_max = 'outgoing-all_180DAY_MAX_CUSTOMER'
    incoming_all_360day_amt = 'incoming-all_360DAY_AMT_CUSTOMER'
    outgoing_all_360day_amt = 'outgoing-all_360DAY_AMT_CUSTOMER'
    incoming_all_12month_monthly_avg = 'incoming-all_12MONTH_MONTHLY_AVG_CUSTOMER'
    outgoing_all_12month_monthly_avg = 'outgoing-all_12MONTH_MONTHLY_AVG_CUSTOMER'
    incoming_all_360day_max = 'incoming-all_360DAY_MAX_CUSTOMER'
    outgoing_all_360day_max = 'outgoing-all_360DAY_MAX_CUSTOMER'
    schema = StructType([
        StructField(anomaly_id, StringType()),
        StructField(party_key, StringType()),
        StructField(anomaly_date, TimestampType()),
        StructField(create_date, TimestampType()),
        StructField(cluster_id, StringType()),
        StructField(anomaly_score, DoubleType()),
        StructField(party_age, IntegerType()),
        StructField(is_anomaly, IntegerType()),
        StructField(alert_investigation_result, StringType()),
        StructField(high_risk_incoming_90day_amt, DoubleType()),
        StructField(high_risk_outgoing_90day_amt, DoubleType()),
        StructField(ratio_incoming_all_outgoing_all_90day_amt, DoubleType()),
        StructField(net_incoming_all_outgoing_all_90day_amt, DoubleType()),
        StructField(incoming_local_fund_transfer_90day_amt, DoubleType()),
        StructField(outgoing_local_fund_transfer_90day_amt, DoubleType()),
        StructField(outgoing_fund_transfer_high_risk_country_360day_amt, DoubleType()),
        StructField(cdm_90day_amt, DoubleType()),
        StructField(incoming_overseas_fund_transfer_360day_amt, DoubleType()),
        StructField(outgoing_overseas_fund_transfer_360day_amt, DoubleType()),
        StructField(incoming_all_90day_amt, DoubleType()),
        StructField(outgoing_all_90day_amt, DoubleType()),
        StructField(incoming_all_3month_monthly_avg, DoubleType()),
        StructField(outgoing_all_3month_monthly_avg, DoubleType()),
        StructField(incoming_all_90day_max, DoubleType()),
        StructField(outgoing_all_90day_max, DoubleType()),
        StructField(incoming_all_180day_amt, DoubleType()),
        StructField(outgoing_all_180day_amt, DoubleType()),
        StructField(incoming_all_6month_monthly_avg, DoubleType()),
        StructField(outgoing_all_6month_monthly_avg, DoubleType()),
        StructField(incoming_all_180day_max, DoubleType()),
        StructField(outgoing_all_180day_max, DoubleType()),
        StructField(incoming_all_360day_amt, DoubleType()),
        StructField(outgoing_all_360day_amt, DoubleType()),
        StructField(incoming_all_12month_monthly_avg, DoubleType()),
        StructField(outgoing_all_12month_monthly_avg, DoubleType()),
        StructField(incoming_all_360day_max, DoubleType()),
        StructField(outgoing_all_360day_max, DoubleType()),
    ])


class CDD_SUPERVISED_OUTPUT:
    # intermidiate table
    alert_id = 'ALERT_ID'
    party_key = 'PARTY_KEY'
    alert_created_date = 'ALERT_CREATED_DATE'
    cluster_id = 'Combined_ID'
    prediction_score = 'Prediction_Prob_1_xgb'
    prediction_bucket = 'alertPriority_xgb'
    risk_bucket = 'amls_crs_risk'
    party_age = 'PARTY_AGE'
    high_risk_incoming_90day_amt = 'high-risk-incoming_90DAY_AMT_CUSTOMER'
    high_risk_outgoing_90day_amt = 'high-risk-outgoing_90DAY_AMT_CUSTOMER'
    ratio_incoming_all_outgoing_all_90day_amt = 'ratio_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER'
    net_incoming_all_outgoing_all_90day_amt = 'net_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER'
    incoming_local_fund_transfer_90day_amt = 'incoming-local-fund-transfer_90DAY_AMT_CUSTOMER'
    outgoing_local_fund_transfer_90day_amt = 'outgoing-local-fund-transfer_90DAY_AMT_CUSTOMER'
    outgoing_fund_transfer_high_risk_country_360day_amt = 'outgoing-fund-transfer_360DAY_TO-HIGH-RISK-COUNTRY_AMT_CUSTOMER'
    cdm_90day_amt = 'CDM-cash-deposit_90DAY_AMT_CUSTOMER'
    incoming_overseas_fund_transfer_360day_amt = 'incoming-overseas-fund-transfer_360DAY_AMT_CUSTOMER'
    outgoing_overseas_fund_transfer_360day_amt = 'outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER'
    incoming_all_90day_amt = 'incoming-all_90DAY_AMT_CUSTOMER'
    outgoing_all_90day_amt = 'outgoing-all_90DAY_AMT_CUSTOMER'
    incoming_all_3month_monthly_avg = 'incoming-all_3MONTH_MONTHLY_AVG_CUSTOMER'
    outgoing_all_3month_monthly_avg = 'outgoing-all_3MONTH_MONTHLY_AVG_CUSTOMER'
    incoming_all_90day_max = 'incoming-all_90DAY_MAX_CUSTOMER'
    outgoing_all_90day_max = 'outgoing-all_90DAY_MAX_CUSTOMER'
    incoming_all_180day_amt = 'incoming-all_180DAY_AMT_CUSTOMER'
    outgoing_all_180day_amt = 'outgoing-all_180DAY_AMT_CUSTOMER'
    incoming_all_6month_monthly_avg = 'incoming-all_6MONTH_MONTHLY_AVG_CUSTOMER'
    outgoing_all_6month_monthly_avg = 'outgoing-all_6MONTH_MONTHLY_AVG_CUSTOMER'
    incoming_all_180day_max = 'incoming-all_180DAY_MAX_CUSTOMER'
    outgoing_all_180day_max = 'outgoing-all_180DAY_MAX_CUSTOMER'
    incoming_all_360day_amt = 'incoming-all_360DAY_AMT_CUSTOMER'
    outgoing_all_360day_amt = 'outgoing-all_360DAY_AMT_CUSTOMER'
    incoming_all_12month_monthly_avg = 'incoming-all_12MONTH_MONTHLY_AVG_CUSTOMER'
    outgoing_all_12month_monthly_avg = 'outgoing-all_12MONTH_MONTHLY_AVG_CUSTOMER'
    incoming_all_360day_max = 'incoming-all_360DAY_MAX_CUSTOMER'
    outgoing_all_360day_max = 'outgoing-all_360DAY_MAX_CUSTOMER'
    schema = StructType([
        StructField(alert_id, StringType()),
        StructField(party_key, StringType()),
        StructField(alert_created_date, TimestampType()),
        StructField(cluster_id, StringType()),
        StructField(prediction_score, DoubleType()),
        StructField(prediction_bucket, StringType()),
        StructField(risk_bucket, StringType()),
        StructField(party_age, IntegerType()),
        StructField(high_risk_incoming_90day_amt, DoubleType()),
        StructField(high_risk_outgoing_90day_amt, DoubleType()),
        StructField(ratio_incoming_all_outgoing_all_90day_amt, DoubleType()),
        StructField(net_incoming_all_outgoing_all_90day_amt, DoubleType()),
        StructField(incoming_local_fund_transfer_90day_amt, DoubleType()),
        StructField(outgoing_local_fund_transfer_90day_amt, DoubleType()),
        StructField(outgoing_fund_transfer_high_risk_country_360day_amt, DoubleType()),
        StructField(cdm_90day_amt, DoubleType()),
        StructField(incoming_overseas_fund_transfer_360day_amt, DoubleType()),
        StructField(outgoing_overseas_fund_transfer_360day_amt, DoubleType()),
        StructField(incoming_all_90day_amt, DoubleType()),
        StructField(outgoing_all_90day_amt, DoubleType()),
        StructField(incoming_all_3month_monthly_avg, DoubleType()),
        StructField(outgoing_all_3month_monthly_avg, DoubleType()),
        StructField(incoming_all_90day_max, DoubleType()),
        StructField(outgoing_all_90day_max, DoubleType()),
        StructField(incoming_all_180day_amt, DoubleType()),
        StructField(outgoing_all_180day_amt, DoubleType()),
        StructField(incoming_all_6month_monthly_avg, DoubleType()),
        StructField(outgoing_all_6month_monthly_avg, DoubleType()),
        StructField(incoming_all_180day_max, DoubleType()),
        StructField(outgoing_all_180day_max, DoubleType()),
        StructField(incoming_all_360day_amt, DoubleType()),
        StructField(outgoing_all_360day_amt, DoubleType()),
        StructField(incoming_all_12month_monthly_avg, DoubleType()),
        StructField(outgoing_all_12month_monthly_avg, DoubleType()),
        StructField(incoming_all_360day_max, DoubleType()),
        StructField(outgoing_all_360day_max, DoubleType()),
    ])


class CDD_SAMPLING_PARAMS:
    # TM Sampling Params table
    model_id = 'MODEL_ID'
    alert_id = 'ALERT_ID'
    train_max_date = 'TRAIN_MAX_DATE'
    original_sampling_params = 'ORIGINAL_SAMPLING_PARAMS'
    actual_sampling_params = 'ACTUAL_SAMPLING_PARAMS'
    created_timestamp = 'CREATED_TIMESTAMP'
    tt_updated_year = 'TT_UPDATED_YEAR'
    schema = StructType([
        StructField(model_id, IntegerType()),
        StructField(alert_id, StringType()),
        StructField(train_max_date, TimestampType()),
        StructField(original_sampling_params, StringType()),
        StructField(actual_sampling_params, StringType()),
        StructField(created_timestamp, TimestampType()),
        StructField(tt_updated_year, IntegerType())
    ])


class FEATURE_GROUP:
    # Feature Group Table
    type_identifier = 'TYPE_IDENTIFIER'
    feature_name = 'FEATURE_NAME'
    group_name = 'GROUP_NAME'
    created_datetime = 'CREATED_DATETIME'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'
    schema = StructType([
        StructField(type_identifier, StringType()),
        StructField(feature_name, StringType()),
        StructField(group_name, IntegerType()),
        StructField(created_datetime, TimestampType()),
        StructField(tt_updated_year_month, IntegerType())
    ])


class FEATURE_TYPOLOGY:
    # Feature Typology Table
    feature_name = 'FEATURE_NAME'
    typology_name = 'TYPOLOGY_NAME'
    created_datetime = 'CREATED_DATETIME'
    tt_updated_year_month = 'TT_UPDATED_YEAR_MONTH'
    schema = StructType([
        StructField(feature_name, StringType()),
        StructField(typology_name, StringType()),
        StructField(created_datetime, TimestampType()),
        StructField(tt_updated_year_month, IntegerType())
    ])


class CDD_THRESHOLD:
    # TM Threshold Table
    model_id = 'MODEL_ID'
    l1_threshold = 'L1_THRESHOLD'
    l3_threshold = 'L3_THRESHOLD'
    performance_measure = 'PERFORMANCE_MEASURE'
    created_timestamp = 'CREATED_TIMESTAMP'
    tt_updated_year = 'TT_UPDATED_YEAR'
    schema = StructType([
        StructField(model_id, IntegerType()),
        StructField(l1_threshold, DoubleType()),
        StructField(l3_threshold, DoubleType()),
        StructField(performance_measure, StringType()),
        StructField(created_timestamp, TimestampType()),
        StructField(tt_updated_year, IntegerType())
    ])
