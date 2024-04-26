try:
    from prepipeline_tables import TMALERTS
except ImportError as e:
    from TransactionMonitoring.src.tm_feature_engineering.prepipeline_tables import TMALERTS


class Defaults:
    LEFT_JOIN = 'left'
    RIGHT_JOIN = 'right'
    LEFT_ANTI_JOIN = 'left_anti'
    BOOLEAN_TRUE = True
    BOOLEAN_FALSE = False
    STRING_OPP_ = 'opp_'
    STRING_TRANSACTIONS = 'TRANSACTIONS'
    STRING_ALERTS = 'ALERTS'
    STRING_TXN_ALERT_ID = 'txn_alert_id'
    STRING_TXN_ALERT_DATE = 'txn_alert_date'
    STRING_ACCOUNT_KEY = 'account_key'
    STRING_PARTY_KEY = 'party_key'
    STRING_TXN_KEY = 'transaction_key'
    STRING_TXN_DATE = 'transaction_date'
    STRING_TXN_CATEGORY = 'transaction_category'
    STRING_TXN_AMOUNT = 'transaction_amount'
    STRING_COMPARE_BOTH_PARTY = 'compare_both_party'
    STRING_FOREIGN_COUNTRY_IND = 'foreign_country_ind'
    STRING_HIGH_RISK_COUNTRY_IND = 'high_risk_country_ind'
    STRING_HIGH_RISK_PARTY_IND = 'high_risk_party_ind'
    STRING_COMPARE_ACCOUNT_BUSINESS_UNIT = 'compare_account_business_unit'
    STRING_OPP_ACC_KEY_NUM = "opp_acc_key_num"
    STRING_COMPARE_ACCOUNT_TYPE_CODE = 'compare_account_type_code'
    STRING_COMPARE_ACCOUNT_SEGMENT_CODE = 'compare_account_segment_code'
    STRING_COMPARE_PARTY_TYPE_CODE = 'compare_party_type_code'
    STRING_PEP_IND = 'pep_ind'
    STRING_ADVERSE_NEWS_IND = 'adverse_news_ind'
    STRING_NGO_IND = 'ngo_ind'
    STRING_INCORPORATE_TAX_HAVEN_IND = "incorporate_tax_haven_ind"
    STRING_OPP_PARTY_AGE_IND = "opp_party_age_ind"
    STRING_OPP_ACC_KEY = 'opp_acc_key'
    STRING_OPP_ACC_NUMBER = 'opp_acc_number'
    STRING_ALERT_ID = 'alert_id'
    STRING_ALERT_DATE = 'alert_date'
    STRING_HISTORY_ALERT_DATE = 'history_alert_date'
    STRING_ALERT_STATUS = 'alert_status'
    STRING_CDD_ALERT_STATUS = 'cdd_alert_status'
    STRING_PARTY_TYPE_CODE = 'party_type_code'
    STRING_OPP_PARTY_TYPE_CODE = 'opp_party_type_code'
    STRING_ACCOUNT_BUSINESS_UNIT = 'account_business_unit'
    STRING_OPP_ACCOUNT_BUSINESS_UNIT = 'opp_account_business_unit'
    STRING_ACCOUNT_SEGMENT_CODE = 'account_segment_code'
    STRING_OPP_ACCOUNT_SEGMENT_CODE = 'opp_account_segment_code'
    STRING_ACCOUNT_TYPE_CODE = 'account_type_code'
    STRING_OPP_ACCOUNT_TYPE_CODE = 'opp_account_type_code'
    ERROR_PREPROCESS_TXN_SELECTION = 'TM_Preprocess.select_table_cols: <TRANSACTIONS> is None'
    ERROR_PREPROCESS_ALERTS_SELECTION = 'TM_Preprocess.select_table_cols: <ALERTS> is None'
    ERROR_PREPROCESS_ACCOUNTS_SELECTION = 'TM_Preprocess.select_table_cols: <ACCOUNTS> is None'
    ERROR_PREPROCESS_CUSTOMERS_SELECTION = 'TM_Preprocess.select_table_cols: <CUSTOMERS> is None'
    ERROR_PREPROCESS_C2A_SELECTION = 'TM_Preprocess.select_table_cols: <C2A> is None'
    ERROR_PREPROCESS_HIGHRISKCOUNTRY_SELECTION = 'TM_Preprocess.select_table_cols: <HIGHRISKCOUNTRY> is None'
    ERROR_PREPROCESS_HIGHRISKPARTY_SELECTION = 'TM_Preprocess.select_table_cols: <HIGHRISKPARTY> is None'
    ERROR_PREPROCESS_ALERTS_HISTORY_SELECTION = 'TM_Preprocess.select_table_cols: <HISTORY_ALERTS> is None'
    ERROR_PREPROCESS_ANOMALY_ALERT_HISTORY_COL_MISMATCH = 'Preprocess: ANOMALY HISTORY DATA NOT ADDED TO ALERT ' \
                                                          'HISTORY due to columns mismatch between both'
    ERROR_PREPROCESS_LESSER_ALERT_COUNT_TRAINING = 'Not sufficient for model training, Alert Count is less than '
    MINIMUM_ALERT_COUNT_FOR_TRAINING = 10
    DISPLAY_PREPROCESS_MSG_NO_DERIVED_TXN_FEATURES = 'No derived transaction features added in join by account'
    STRING_HIGHRISK_COUNTRY = 'HIGHRISK_COUNTRY'
    STRING_HIGHRISK_PARTY_FLAG = 'HIGHRISK_PARTY'
    STRING_TRUEALERT_COUNT = 'TRUEALERTS'
    STRING_ALERT_COUNT = 'ALERTS'
    STRING_TRUEALERT_CUSTOMER_FLAG = 'TRUEALERT_CUSTOMER'
    STRING_HIGH_RISK_PARTY = 'HIGH_RISK_PARTY'
    HIGH_RISK_OCCUPATION_FLAG = 'HIGH_RISK_OCCUPATION'
    HIGH_RISK_BUSINESS_TYPE_FLAG = 'HIGH_RISK_BUSINESS_TYPE'
    INFO_PREPROCESS_ACCOUNT_MODE_NO_FILTERING = 'join_by_account: No filtering needed on alerts table'
    INFO_PREPROCESS_CUSTOMER_MODE_NO_FILTERING = 'join_by_customer: No filtering needed on alerts table'
    INFO_PREPROCESS_ACCOUNT_MODE_NO_FILTER_MISSING_TXNS = 'join_by_account: missing transactions for accounts is ' \
                                                          'acceptable'
    MIN_LOOKBACK_DATE = 'MIN_LOOKBACK_' + TMALERTS.alert_created_date
    MSG_PREPROCESS_PREDICT_MODE = 'Predict Preprocessing module'
    MSG_PREPROCESS_TRAIN_MODE = 'Train Preprocessing module'
    TYPE_INT = 'int'
    TYPE_JSON = 'json'
    TYPE_STRING = 'string'
    TYPE_TIMESTAMP = 'timestamp'
    TYPE_DATE = 'date'
    INTEGER_1 = 1
    INTEGER_0 = 0
    STRING_1 = '1'
    STRING_0 = '0'
    HIGHRISK_COUNTRY_FLAG = 'HIGHRISK_COUNTRY'
    HIGHRISK_PARTY_FLAG = 'HIGHRISK_PARTY'
    TRUEALERT_COUNT = 'TRUEALERTS'
    ALERT_COUNT = 'ALERTS'
    TRUEALERT_CUSTOMER_FLAG = 'TRUEALERT_CUSTOMER'
    STRING_NUMBER_OF_CUSTOMERS = 'NUMBER_OF_CUSTOMERS'
    STRING_LINKED_ = 'LINKED_'
    STRING__COUNT = '_COUNT'
    STRING_NUMBER_OF_ACCOUNTS = 'NUMBER_OF_ACCOUNTS'
    STRING_PARTY_AGE = 'PARTY_AGE'
    STRING_OPP_PARTY_AGE = 'OPP_PARTY_AGE'
    CONSTANT_YEAR_DAYS = 365.25
    STRING_ACCOUNT_AGE = 'ACCOUNT_AGE'
    STRING_ROW = 'row'
    STRING_SELECT_ALL = '*'
    STRING_NULL = 'null'
    STRING_EXPIRATION_DATE = 'expiration_date'
    STRING_PREDEFINED_COMMENTS = 'predefined_comments'
    ERROR_UI_ALERTED_CUSTOMERS_BOTH_SUPERVISED_ANOMALY_INPUT_PROVIDED = 'CreateAlertedCustomers: both supervised ' \
                                                                        'output and anomaly df are provided and not ' \
                                                                        'expected'
    EXPIRATION_DATE_COL_FORMAT = "YYYY-MM-dd'T'HH:mm:ss.SSS'Z'" #24 hour format [15:22 instead of 03:22]
    WARNING_COLS_MISSING = 'there are selected columns that the dataframe DOES NOT HAVE! -- '
    STRING_HISTORY_ALERT_CREATED_DATE = 'HISTORY_' + TMALERTS.alert_created_date
    SUPERVISED_MAX_HISTORY = 360
    CREATED_USER = 'TM_PREDICTION_PIPELINE'
    UPDATED_USER = 'TM_DELTA_PIPELINE'
    IS_STR_MAPPING = {'Non-STR': 0, 'STR': 1, 'Undetermined': 0}
    IS_ISSUE_MAPPING = {'Non-STR': 0, 'STR': 1, 'Undetermined': 2}
    ALERT_STATUS_MAPPING = {'Non-STR': 1, 'STR': 1, 'Undetermined': 1}
    CRS_ALERT_MAPPING = {'Non-STR': 0, 'False-Alert': 0, '0': 0, 'STR': 1, 'True-Alert': 1, '1': 1}
    HIVE_HBASE_MAPPING = {'BANK_ANALYST_USER_NAME': 'ANALYST_NAME',
                          'ALERT_CLOSED_DATE': 'CLOSURE_DATE',
                          'ALERT_CREATED_DATE': 'ALERT_DATE',
                          'ALERT_ID': 'ALERT_ID',
                          'ALERT_INVESTIGATION_RESULT': 'INVESTIGATION_RESULT'}
    DATEFORMAT_YYYY_MM_DD = '%Y-%m-%d'
    DATEFORMAT_YYYY_MM_DD_HH_MM_SS = '%Y-%m-%d %H:%M:%S'
    STRING_TYPOLOGY = 'TYPOLOGY'
    STRING_CREATED_DATETIME = 'CREATED_DATETIME'
    SAMPLING_SEED_NUMBER = 42
    DEFAULT_VALID_PERC = 20.0
    DEFAULT_MAX_ALLOWED_VALID_SIZE = 45.0
    DEFAULT_MIN_ALLOWED_VALID_SIZE = 5.0
    DEFAULT_OLD_VALID_PERC = 100.0
    STRING_RANDOM_SAMPLING = 'RANDOM_SAMPLING'
    STRING_TARGET_STRATIFIED_SAMPLING = 'TARGET_STRATIFIED_SAMPLING'
    STRING_LOOKBACK_STRATIFIED_SAMPLING = 'LOOKBACK_STRATIFIED_SAMPLING'
    STRING_LOOKBACK_MEMORY_STRATIFIED_SAMPLING = 'LOOKBACK_MEMORY_STRATIFIED_SAMPLING'
    SAMPLING_MODES = [STRING_RANDOM_SAMPLING, STRING_TARGET_STRATIFIED_SAMPLING, STRING_LOOKBACK_STRATIFIED_SAMPLING,
                      STRING_LOOKBACK_MEMORY_STRATIFIED_SAMPLING]
    ERROR_WRONG_SAMPLING_MODE = 'Wrong Expected sampling mode'
    INFO_DATATYPE_MISMATCH = 'Datatype Mismatch'
    ERROR_ACTUAL_EXPECTED = 'Error Actual = {}, Expected - {}'
    PARAM_KEY_TARGET_COL = 'target_col'
    PARAM_KEY_ID_COL = 'id_col'
    PARAM_KEY_VALID_PERC = 'valid_perc'
    PARAM_KEY_REF_DATE_COL = 'ref_date_col'
    PARAM_KEY_PREV_TRAIN_MAX_DATE = 'prev_train_max_date'
    PARAM_KEY_VALID_LOOKBACK_MONTHS = 'valid_lookback_months'
    PARAM_KEY_OLD_VALID_PERC = 'old_valid_perc'
    PARAM_KEY_PREV_ALERTS = 'prev_alerts'
    ERROR_DF_LEN_LTEQ_1 = 'df len is <= 1 '
    INFO_ALL_2_TRAIN = 'Included all data to training '
    MT_STR = ''
    INFO_TRY_RANDOM_SAMPLING = ' - Trying Random Sampling'
    INFO_PREV_CURR_TRAIN_EQUAL = "Previous Max Train date is equal current data max date. Hence Old Valid perc is " \
                                 "defaulted to {}".format(DEFAULT_OLD_VALID_PERC)
    STR_DATASHIFT_DF = 'DataShift_df: '
    STR_OLDVALID_DF = 'OldValid_df: '
    STR_IS_NONE = 'is None'
    INFO_VALID_PERC_LT_ALLOWED_MIN = 'Valid cannot be less than min allowed. Hence defaulting to {}'
    INFO_ADJUST_VALID_PERC_BASED_ON_MAX = 'Valid cannot be more than max allowed. Hence defaulting to {}'
    MSG_SEPERATOR = '%%'
    STR_IS_LT_ZERO = 'is < 0'
    INFO_ACTUAL_VALID_PERC_IN_LOOKBACK_GT_EXP_VALUE = 'Valid size from lookback data size is Greater than allowed ' \
                                                      'max, hence correcting to accepted max'
    INFO_ACTUAL_VALID_PERC_IN_LOOKBACK_LT_EXP_VALUE = 'Valid size from lookback data size is less than allowed ' \
                                                      'min, hence correcting to accepted min'
    INFO_OUTSIDE_RANGE = 'Outside Range'
    INFO_NOT_PRESENT = 'not present'
    INFO_ONLY_1_TARGET = 'Target Column contains only 1 unique value'
    INFO_TARGET_LT_2 = 'One of the target value count is less than 2'
    FATAL_DATA_SHIFT_BACKWARD = 'Current Max date in the training data is less than Previous training Max date. ' \
                                'Aborting the process. Please correct the data / adjust the connector date ' \
                                'if wrong input is provided'
    FATAL_TARGET_DIST = 'Aborting: Target distribution is not proper. Least possible mode (Target Stratified ' \
                        'Sampling) also failed - {}'
    INFO_FULL_DATASHIFT = 'Current Min date is greater than previous max date'
    INFO_LOOKBACK_RANGE_SMALL = 'Total data range is more than 3 months and Valid lookback of < 0.5 month is not accepted'
    INFO_LOOKBACK_LEQ_ZERO = '<= 0 month'
    INFO_LOOKBACK_OUTSIDE_RANGE = 'Lookback range is refering more than data range. Hence adjusting to possible max'
    INFO_LIST_LEN_0 = 'Length of list is Zero'
    TM_XGB_MODEL_ID = '_modelJobId_xgb'
    PIPE_CONTEXT_CURR_MODEL_DETS = 'currentModelDetails'
    PIPE_CONTEXT_CURR_MODEL_SL_DETS = 'selfLearningModelDetails'
    PIPE_CONTEXT_CURR_CHALL = 'currentModelId'
    PIPE_CONTEXT_CURR_CHAMP = 'championModelId'
    PIPE_CONTEXT_PREV_SL_DETS = 'latestSuccessfulModelDetails'
    PIPE_CONTEXT_PREV_SL_CHAMP = 'previousChampionModel'
    STRING_ACTUAL = 'actual'
    STRING_INPUT = 'input'
    MAX_ALLOWED_RANGE_NOT_SATISFIED = 'Max allowed valid percentage can be between 35 to 50% only'
    STR_ALERT_MODE = 'ALERT_MODE'
    STR_ANOMALY_MODE = 'ANOMALY_MODE'
    INFO_DEFAULT_SAMPLING_SWITCH = 'Input Sampling mode is not matching any of the possible modes. ' \
                                   'Hence defaulting to {}'
    struct_range_string = '{}kto{}k'
    struct_greater_string = 'greater-than-{}k'
    age_condition_to_filter_key = 'OPP-PARTY-AGE-UNDER-{}'
    age_condition_to_filter_value = 'oppPartyAge<{}'
    TABLE_TYPOLOGY_FEATURE_MAPPING = 'TYPOLOGY_FEATURE_MAPPING'
    TABLE_CODES = 'CODES'
    STR_FEATURE_NAME = 'FEATURE_NAME'
    DYN_PROP_UDF_CATEGORY = 'UDF_CATEGORY'
    STR_TM_ALERT_PRIORITISATION_CHAMPION = 'TM_ALERT_PRIORITISATION_CHAMPION'
    STR_CRS_ALERT_PRIORITISATION_CHAMPION = 'CRS_ALERT_PRIORITISATION_CHAMPION'
    STR_TM_SAMPLING_CONFIG = 'TM_SAMPLING_CONFIG'
    STR_CRS_SAMPLING_CONFIG = 'CRS_SAMPLING_CONFIG'
    STR_PREV_VALID_DF = 'PREV_VALID_DF'
    INFO_FROM_DYN_PROPS = 'CHAMPION_MODEL_ID from Dynamic props - {}'
    INFO_FROM_PIPELINE_CONTEXT = 'CHAMPION_MODEL_ID from Pipeline Context - {}'
    STR_PIPELINE_CONTEXT = 'PIPELINE_CONTEXT'
    SAMP_CONF_VALID_LOOKBACK_MONTHS = 'VALID_LOOKBACK_MONTHS'
    SAMP_CONF_VALID_PERCENTAGE = 'VALID_PERCENTAGE'
    SAMP_CONF_OLD_VALID_PERCENTAGE = 'OLD_VALID_PERCENTAGE'
    SAMP_CONF_MAX_VALID_SIZE_PERCENTAGE = 'MAX_VALID_SIZE_PERCENTAGE'
    SAMP_CONF_SAMPLING_MODE = 'SAMPLING_MODE'
    CONSERVATIVE_REDUCTION_CUT = 0.45
    L3_RECALL = 0.80
    MISCLASSIFICATION_TOLERANCE = 0.025
    MISCLASSIFICATION_ALLOWED= 0.025
    FIXED_ALERT_REDUCTION_PERC= 0.45
    PRODUCT_FD_STR = 'FIXED DEPOSITS'
    MAX_ALLOWED_UNSUP_VALIDATION_LOOKBACK_DAYS = 180
    STR_ALERT_PRIORITISATION_THRESHOLD_INDI = 'CRS_ALERT_PRIORITISATION_THRESHOLD_INDI'
    STR_ALERT_PRIORITISATION_THRESHOLD_CORP = 'CRS_ALERT_PRIORITISATION_THRESHOLD_CORP'
    STR_ALERT_PRIORITISATION_THRESHOLD = 'ALERT_PRIORITISATION_THRESHOLD'
    TYPE_FLOAT_LIST = 'float_list'
    INFO_CLIENT_PROVIDED_L1_L3_THRESHOLD = '%%% Applying User Provided L1 L3 Thresholds %%%'
    INFO_AUTO_GEN_L1_L3_THRESHOLD = '%%% Applying Auto Generated L1 L3 Thresholds %%%'
    NUMBER_OF_ACCOUNTS_OPENED_360DAYS = "NUMBER_OF_ACCOUNTS_OPENED_360DAYS"
    NUMBER_OF_ACCOUNTS_OPENED_CLOSED_360DAYS = "NUMBER_OF_ACCOUNTS_OPENED_CLOSED_360DAYS"
    STRING_CDD_ALERT_INVESTIGATION_RESULT = "CDD_ALERT_INVESTIGATION_RESULT"
    LOG_VALID_LOOKBACK_MONTHS = 'Initially, the value of valid_lookback_months is {} . Now, it has been changed to {}'
    LOG_VALID_PERC_LOOKBACK_MONTHS = 'Initially, the value of valid_perc is {} . Now, it has been changed to {}'
    SKIP_SKEW_DATA_FLAG = "SKIP_SKEW_DATA_FLAG"
    SALT_TXN_COUNT_THRESHOLD = "SALT_TXN_COUNT_THRESHOLD"
    SALT_REPARTITION_NUMBER = "SALT_REPARTITION_NUMBER"

    RULE_STRING_SPLIT_VAL = ';'

    RATIO_FD_BALANCE_FD_AMOUNT = 'ratio_fd-balance_fd-amount'
    FD_ALERT_DATE = "fd_alert_date"
    RATIO_LOAN_BALANCE_LOAN_AMOUNT = 'ratio_loan-balance_loan-amount'
    DATE_DIFF = 'date_diff'
    LOAN_ALERT_DATE = "loan_alert_date"
    RATIO_LOAN_AGE_LOAN_PERIOD = 'ratio_loan-age_loan-period'
    TEMP_NAME = 'temp_name'
    CREATE_DATE = "CREATE_DATE"
    ALL_CATEGORY = "ALL-CATEGORY"
    LOAN_AMOUNT = "LOAN-AMOUNT-OF_"
    LOAN_AGE = 'LOAN_AGE'
    LOAN_AGE_NEW_EF = 'loan_age'
    ACCOUNT = "ACCOUNT"
    CUSTOMER = "CUSTOMER"
    RATIO_FD_AGE_FD_PERIOD = 'ratio_fd-age_fd-period'
    COUNT_LOANS = "COUNT_LOANS"
    COUNT_FD = "COUNT_FD"
    FD_AGE = 'FD_AGE'
    MAX_RANGE_DAYS = 100000000
    EARLY_WITHDRAW_FLAG = "EARLY_WITHDRAW_FLAG"

    EXCLUSION_TRAIN_FEATURE_LIST = "EXCLUSION_TRAIN_FEATURE_LIST"
    SKIP_LOAN_FEATURE_FLAG = "SKIP_LOAN_FEATURE_FLAG"
    SKIP_FD_FEATURE_FLAG = "SKIP_FD_FEATURE_FLAG"
    SKIP_LOAN_FEATURE_FLAG_PREDICT = "SKIP_LOAN_FEATURE_FLAG_PREDICT"
    SKIP_FD_FEATURE_FLAG_PREDICT = "SKIP_FD_FEATURE_FLAG_PREDICT"

    FEATURE_FILL_VALID_DF_FLAG = "FEATURE_FILL_VALID_DF_FLAG"


    #CRS SAMPLING AND CLUSTERING RELATED CONSTANTS:
    ACTUAL_CLUSTERS = "ACTUAL_CLUSTERS"
    PREDICTIONS_COL = "PREDICTIONS_COL"
    STRATIFY_SAMPLE_TYPE = "stratify"
    MAIN_SAMPLE_TYPE = "sample_type"
    STRATIFY_COL = "stratify_col"
    STRATIFY_PERCENTAGE = "stratify_per"
    RANDOM_SAMPLE_PERCENTAGE = "random_sample_per"
    MINIMUM_SAMPLES = "minimum_samples"
    CRS_CLUSTER_FEATURES = "crs_cluster_features"
    DBSCAN_EPS = "dbscan_eps"
    WINDOW_LENGTH = "window_length"
    DBSCAN_MINIMUM_SAMPLES = "dbscan_minimum_samples"
    MINIMUM_SAMPLES_VALUE = 1000
    STRATIFY_SAMPLING_VALUE = 1.0
    RANDOM_SAMPLING_PERCENTAGE_VALUE = 0.25
    MISSING_PERCENTAGE = 25
    MISSING_THRESHOLD_PERCENTAGE = "missing_threshold_percentage"
    CH_INDEX_SCORE = "ch_index_score"
    DAVIES_BOULDIN_SCORE = "davies_bouldin_score"
    SILHOUETTE_SCORE = "silhouette_score"
    METRIC = "metric"
    SCORE= "score"
    PATH_TO_SAVE_PLOTS = "path_to_save_plots"
    MAX_STAGNATION = "max_stagnation"
    TUNING_TECHNIQUE = "tuning_technique"
    KNEE_TECHNIQUE = "knee_technique"
    ML_TECHNIQUE = "ml_technique"

