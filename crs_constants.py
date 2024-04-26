CUSTOMER_TRANSACTION_BEHAVIOUR = 'CUSTOMER_TRANSACTION_BEHAVIOUR'
KNOW_YOUR_CUSTOMER = 'KYC'
SCREENING = 'SCR'
NETWORK_LINK_ANALYSIS = 'NLA'
ACCOUNTS_TYPE_CODE = 'ACCOUNTS_TYPE_CODE'
SEGMENT_CODE = 'SEGMENT_CODE'


class RISK_CATEGORY:
    high = "HIGH"
    low = "LOW"
    medium = "MEDIUM"


class STR_LABEL:
    STR = "STR"
    NON_STR = "Non-STR"


class CRS_Default:

    HIGH_CONSERVATIVE_RATIO = 0.1
    LOW_CONSERVATIVE_RATIO = 0.4
    DEFAULT_HIGH_RISK_RECALL = 0.8
    DEFAULT_LOW_RISK_MISCLASS = 0.05
    THRESHOLD_CONFLICT_MSG = 'given high risk recall of %f and low risk misclassification of %f, \
            we cannot find reasonable cutoffs. will reset them to default by HIGH_CONSERVATIVE_RATIO and LOW_CONSERVATIVE_RATIO'
    #ENSEMBLE_SCORE_COL = 'Ensemble_Score'
    CUT_OFF_GRANULARITY = 100
    CUT_OFF_FOUND_MSG = 'found reasonable high/low cut off '
    MAX_HIGH_PERC_NO_MET_MSG = 'MAX_PERC_HIGH_RISK(%f) is NOT met, current percentage for high:%f'
    MAX_HIGH_PERC_SATISFIED_MSG = 'MAX_PERC_HIGH_RISK(%f) is satisfied, current percentage for high:%f'
    MIN_LOW_PERC_NO_MET_MSG = 'CRS_LOW_RISK_MISCLASSIFICATION(%f) is NOT met, current percentage for low:%f'
    MIN_LOW_PERC_SATISFIED_MSG = 'CRS_LOW_RISK_MISCLASSIFICATION(%f) is satisfied, current percentage for low:%f'
    COMMENT_TYPE_VAL = 'Recall and Misclass on Validation'
    COMMENT_TYPE_WHOLE = 'On whole population'
    COMMENT_TYPE_THRESHOLD_READJUST = 'Threshold re-adjusted'
    NO_REQUIREMENT_MSG = 'MAX_PERC_HIGH_RISK and MIN_PERC_LOW_RISK is not set. we will use the Threshold directly. '

    COMMENT_HEADER = ['comment_type', 'comment']

    CRS_ALERT_MAPPING = {'True-Alert':'1', 1:'1', 'STR':'1', 'False-Alert':'0', 'Non-STR':'0', 0:'0','1':'1','0':'0'}
    CRS_XGB_MODEL_ID = '_modelJobId_xgb'

    CREATE_TIME_FORMAT = '%d/%M/%Y %H:%m:%s'

    DEFAULT_STATIC_RULE_HIGH_RISK_THRESHOLD = 0.8
    DEFAULT_STATIC_RULE_LOW_RISK_THRESHOLD = 0.2

    DEFAULT_DYNAMIC_RULE_HIGH_RISK_THRESHOLD = 0.8
    DEFAULT_DYNAMIC_RULE_LOW_RISK_THRESHOLD = 0.2

    DEFAULT_UN_SUPERVISE_HIGH_RISK_THRESHOLD = 0.8
    DEFAULT_UN_SUPERVISE_LOW_RISK_THRESHOLD = 0.2

    DEFAULT_SUPERVISE_HIGH_RISK_THRESHOLD = 0.8
    DEFAULT_SUPERVISE_LOW_RISK_THRESHOLD = 0.2

    DEFAULT_MAX_PERC_HIGH_RISK = 0.05
    DEFAULT_MIN_PERC_LOW_RISK = 0.7

    RISK_LEVEL_COL = 'RISK_LEVEL_CATEGORICAL'
    RISK_LEVEL = 'RISK_LEVEL'
    DEFAULT_DECILE_NUM = 100
    OVERALL_RISK_SCORE_COL = 'OVERALL_RISK_SCORE'
    ACTUAL_LABEL_COL = 'ALERT_INVESTIGATION_RESULT'
    HIGH_RISK_THRESHOLD_COL = 'HIGH_RISK_THRESHOLD'
    LOW_RISK_THRESHOLD_COL = 'LOW_RISK_THRESHOLD'
    THRESHOLD_TYPE_COL = 'THRESHOLD_TYPE'
    MODEL_ID = "MODEL_ID"
    PARTY_KEY_COL = 'PARTY_KEY'
    PRIMARY_PARTY_KEY_COL = 'PRIMARY_PARTY_KEY'

    TRANSACTION_RULES_IDENTIFIER = "Transaction Behaviour"
    ALIAS_TRANSACTION_TYPE = "ALIAS_TRANSACTION_TYPE"
    TRANSACTION_TYPE = "TRANSACTION_TYPE"
    TRANSACTION_AMOUNTS_RULES_IDENTIFIER = "TRANSACTION AMOUNTS"
    TRANSACTION_VOLUMES_RULES_IDENTIFIER = "TRANSACTION VOLUMES"
    SUB_CONDITION = "SUB_CONDITION"
    TRANSACTION_AMOUNT = "TRANSACTION_AMOUNT"
    AGGREGATION_FUNCTION = "AGGREGATION_FUNCTION"
    TT_BEHAVIOUR_RULES_IDENTIFIER = "BEHAVIOUR_RULES_"
    TT_STATIC_RULES_IDENTIFIER = "STATIC_RULES_"
    BUSINESS_REGISTRATION_DOCUMENT_EXPIRED_AGE = "BUSINESS_REGISTRATION_DOCUMENT_EXPIRED_AGE"
    ADDITIONAL_FILTERS = "ADDITIONAL_FILTERS"
    INCOMING_ALL = "INCOMING_ALL"
    OUTGOING_ALL = "OUTGOING_ALL"
    Incoming = "Incoming"
    Outgoing = "Outgoing"
    Maximum = "Maximum"
    Volume = "Volume"
    Sum = "Sum"
    All = "All"
    TT_UI_RISK_INDICATOR_SPLITTER = "over past "
    TT_UI_DAYS_SPLITTER = " Days"
    TT_UI_DAY_SPLITTER = " Day"
    RISK_INDICATOR = "RISK_INDICATOR"
    RULE_ID = "RULE_ID"
    MAPPINGS = "MAPPINGS"
    CONDITION_UNIQUE_IDENTIFIER = "CONDITION_UNIQUE_IDENTIFIER"
    CREATED_TIME = "CREATED_TIME"
    LOOK_BACK = "LOOK_BACK"
    IN = " IN "
    AND = " AND "
    OR = " OR "
    BETWEEN = " BETWEEN "
    ALL = "ALL"
    ANY = "ANY"
    ANY1 = " ANY"
    ALL1 = " ALL"
    TT_RULE_NO_IDENTIFIER = "TT_RULE_NO_"
    TT_RULE_ID = "RULE_ID"
    TT_RISK_SCORE_IDENTIFIER = "RISK_SCORE_FOR_"
    IN_BETWEEN = "IN BETWEEN"
    IN_CONDITION = "IN"
    EQUALS = "=="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_EQUAL = ">="
    LESS_THAN_EQUAL = "<="
    TRUE_IDENTIFIER = "TRUE"
    FALSE_IDENTIFIER = "FALSE"
    INVALID_INDICATOR_CONDITION = "[]"
    OVERALL_RISK_SCORE = "OVERALL_RISK_SCORE"
    DYN_UDF_CATEGORY = "UDF_CATEGORY"
    DYN_CRS_THRESHOLDING_SETTING = "CRS_THRESHOLDING_SETTING"
    DYN_STATIC_RULE_HIGH_RISK_THRESHOLD = "STATIC_RULE_HIGH_RISK_THRESHOLD"
    DYN_STATIC_RULE_LOW_RISK_THRESHOLD = "STATIC_RULE_LOW_RISK_THRESHOLD"
    DYN_DYNAMIC_RULE_HIGH_RISK_THRESHOLD = "DYNAMIC_RULE_HIGH_RISK_THRESHOLD"
    DYN_DYNAMIC_RULE_LOW_RISK_THRESHOLD = "DYNAMIC_RULE_LOW_RISK_THRESHOLD"
    DYN_UN_SUPERVISE_HIGH_RISK_THRESHOLD = "UN_SUPERVISE_HIGH_RISK_THRESHOLD"
    DYN_UN_SUPERVISE_LOW_RISK_THRESHOLD = "UN_SUPERVISE_LOW_RISK_THRESHOLD"
    DYN_SUPERVISE_HIGH_RISK_THRESHOLD = "SUPERVISE_HIGH_RISK_THRESHOLD"
    DYN_SUPERVISE_LOW_RISK_THRESHOLD = "SUPERVISE_LOW_RISK_THRESHOLD"
    DYN_MAX_PERC_HIGH_RISK = "MAX_PERC_HIGH_RISK"
    DYN_MIN_PERC_LOW_RISK = "MIN_PERC_LOW_RISK"
    DYN_CRS_HIGH_RISK_RECALL = "CRS_HIGH_RISK_RECALL"
    DYN_CRS_LOW_RISK_MISCLASSIFICATION = "CRS_LOW_RISK_MISCLASSIFICATION"

    crsCorporate = "corporate"
    crsIndividual = "individual"

    THRESHOLD_TYPE_StaticRule = "STATIC_RULE"#"StaticRule"
    THRESHOLD_TYPE_DynamicRule = "REGULATORY_RULE"#"DynamicRule"
    THRESHOLD_TYPE_UnSupervise = "UNSUPERVISED"#"UnSupervise"
    THRESHOLD_TYPE_Supervise = "SUPERVISED"#"Supervise"

    OVERALL_RISK_SCORE_REGULATORY = "OVERALL_RISK_SCORE_REGULATORY"
    OVERALL_RISK_SCORE_STATIC = "OVERALL_RISK_SCORE_STATIC"
    ANOMALY_SCORE = "ANOMALY_SCORE"
    Prediction_Prob_1_xgb = "Prediction_Prob_1_xgb"

    crsIndAndCorIdentifiers = "crsIndAndCorIdentifiers"
    crsDefaultIndAndCorIdentifiers = {crsIndividual: ["i", crsIndividual], crsCorporate: ["c", crsCorporate]}
    crsSupervisedCorpThresholds = "crsSupervisedCorpThresholds"
    crsSupervisedIndiThresholds = "crsSupervisedIndiThresholds"
    crsSupervisedThresholds = "crsSupervisedThresholds"
    crsUnSupervisedThresholds = "crsUnSupervisedThresholds"
    crsRegulatoryThresholds = "crsRegulatoryThresholds"
    crsStaticThresholds= "crsStaticThresholds"


    crsRiskLevelAggThresholds = "crsRiskLevelAggThresholds"
    crsFinalRiskLevelMode = "crsFinalRiskLevelMode"
    crsFinalRiskLevelModemax = "maximumatflowlevel"
    crsFinalRiskLevelModeVote = "votingatflowlevel"
    crsFinalRiskLevelModeAgg = "aggregationatflowlevel"
    crsUseManualThrdsForValidation = "crsUseManualThrdsForValidation"

    crsRiskLevelAggWeights = "crsRiskLevelAggWeights" # This has to be a proper json with keys as STATIC_RULE, REGULATORY_RULE, UNSUPERVISED, SUPERVISED with the value as the weight we want to give for each flow this will be also used in the generation of the risk level at aggregation mode where we aggregate scores from the 4 flows and determine the risk level based on the new threholds that were provided using(crsRiskLevelAggThresholds)
    crsFinalScoreFlowLevelAgg= "crsFinalScoreFlowLevelAgg" # risllevel, flowlevel-- This parameter tells which method have to be used for final score column geenration if its risklevel the weitage is given as per the risk level from that flow(required CRS_FINAL_SCORE_WEIGHT_SETTING) and if its given flowlevel then the weights are to be given each flow(requires crsRiskLevelAggWeights), if nothing is given it normal weighted average acorss the risk levels of 4 flows

    raw_static_flow = "raw_static_flow"
    raw_regulatory_flow = "raw_regulatory_flow"
    raw_avg_static_regulatory_flow = "raw_avg_static_regulatory_flow"
    CRS_RULES_NORMALIZATION_FACTOR = "CRS_RULES_NORMALIZATION_FACTOR"
    CRS_RULES_NORMALIZATION_FLAG = "CRS_RULES_NORMALIZATION_FLAG"

    crsTempEngNameMappingScoring = "crsTempEngNameMappingScoring"

    THRESHOLD_HIGH = "High"
    THRESHOLD_MEDIUM = "Medium"
    THRESHOLD_LOW = "Low"
    ALERT_LEVEL_GOOD = "Good"

    THRESHOLD_DATE_COL = "THRESHOLD_DATE"

    DECILE_COL = "decile"
    DECILE_NUM_OF_TOTAL_ACCOUNT = "num_of_total_account"
    DECILE_NUM_OF_TRUE_ALERTS = "num_of_true_alerts"
    DECILE_NUM_OF_FALSE_ALERTS = "num_of_false_alerts"
    DECILE_MIN_SCORE = "min_score"
    DECILE_MAX_SCORE = "max_score"
    DECILE_CUM_SUM_OF_TOTAL_ACCOUNT = "cum_sum_of_total_account"
    DECILE_CUM_PER_OF_TOTAL_ACCOUNT = "cum_percentage_of_total_account"
    DECILE_CUM_SUM_OF_TRUE_ALERTS = "cum_sum_of_true_alerts"
    DECILE_CUM_PER_OF_TRUE_ALERTS = "cum_percentage_of_true_alerts"

    OVERALL_RISK_SCORE_STATIC_RULE_COL = "OVERALL_RISK_SCORE_STATIC_RULE"
    RISK_LEVEL_STATIC_RULE_COL = "RISK_LEVEL_STATIC_RULE"
    OVERALL_RISK_SCORE_DYNAMIC_RULE_COL = "OVERALL_RISK_SCORE_DYNAMIC_RULE"
    RISK_LEVEL_DYNAMIC_RULE_COL = "RISK_LEVEL_DYNAMIC_RULE"
    OVERALL_RISK_SCORE_UN_SUPERVISE_COL = "OVERALL_RISK_SCORE_UN_SUPERVISE"
    RISK_LEVEL_UN_SUPERVISE_COL = "RISK_LEVEL_UN_SUPERVISE"
    OVERALL_RISK_SCORE_SUPERVISE_COL = "OVERALL_RISK_SCORE_SUPERVISE"
    RISK_LEVEL_SUPERVISE_COL = "RISK_LEVEL_SUPERVISE"

    ENSEMBLE_RISK_LEVEL_COL = "ENSEMBLE_RISK_LEVEL"
    MAX_SCORE_COL = "MAX_SCORE"
    MIN_SCORE_COL = "MIN_SCORE"
    AVG_SCORE_COL = "AVG_SCORE"
    ENSEMBLE_SCORE_COL = "ENSEMBLE_SCORE"

    ENSEMBLE_RISK_LEVEL = "ENSEMBLE_RISK_LEVEL"

    DYN_CRS_FINAL_SCORE_WEIGHT_SETTING = "CRS_FINAL_SCORE_WEIGHT_SETTING"
    DYN_CRS_FINAL_SCORE_WEIGHT_HIGH = "WEIGHT_HIGH"
    DYN_CRS_FINAL_SCORE_WEIGHT_MEDIUM = "WEIGHT_MEDIUM"
    DYN_CRS_FINAL_SCORE_WEIGHT_LOW = "WEIGHT_LOW"

    DEFAULT_FINAL_SCORE_WEIGHT_HIGH = 1.0
    DEFAULT_FINAL_SCORE_WEIGHT_MEDIUM = 1.0
    DEFAULT_FINAL_SCORE_WEIGHT_LOW = 1.0
    NR_MODE = "NR_MODE"
    ALL_CUSTOMER_MODE = "ALL_CUSTOMER_MODE"

    explainabilty_category_mapping = {THRESHOLD_TYPE_StaticRule: "STATIC",
                                      THRESHOLD_TYPE_DynamicRule: "REGULATORY",
                                      THRESHOLD_TYPE_Supervise: "HISTORICAL BEHAVIOUR",
                                      THRESHOLD_TYPE_UnSupervise: "INCONSISTENCY BEHAVIOUR"}
    STATIC_RULES = "Static"
    BEHAVIOUR_RULES = "Regulatory"

    FEATURE_CATEGORY_MAPPING_DICT = {"Static": "STATIC_RULES", "Regulatory": "BEHAVIOUR_RULES", "STATIC": "STATIC_RULES", "REGULATORY": "BEHAVIOUR_RULES"}

    TRANSACTION_VOL_VAL_DIMENSION = "Transactions Volume Value Dimension"
    COUNTER_PARTY_DIMENSION = "Counter Party Transactions Dimension"
    HISTORICAL_BEHAVIOUR_DIMENSION = "Historical Behaviour Dimension"
    UNKNOWN_DIMENSION = "Unknown Dimension"


    isRuleBased = "isRuleBased"
    contextKey = "contextKey"
    userThresholdMap = "userThresholdMap"
    iadScoringTypologyFeatureMap = "iadScoringTypologyFeatureMap"
    iadClusteringTypologyFeatureMap = "iadClusteringTypologyFeatureMap"
    businessTechFeatureMap = "businessTechFeatureMap"
    modelThresholdMap = "modelThresholdMap"
    typologyThresholdMap = "typologyThresholdMap"
    typologyScenario = "typologyScenario"
    typologyFeatureMap = "typologyFeatureMap"
    name = "name"
    scenarioInstanceId = "scenarioInstanceId"
    typologies = "typologies"
    createdTime= "createdTime"
    id_ = "id"
    TITLE = "title"
    DESCRIPTION = "description"
    TECHNIQUE = "technique"
    LOCATION = "location"
    PREDICATE_OFFENCE = "predicateOffence"
    SEGMENT = "segment"
    FINANCIAL_INSTRUMENT = "financialInstrument"
    CREATED_TIME = "createdTime"
    UPDATED_TIME = "updatedTime"
    IS_APPROVED_FOR_ANOMALY_TRAINING = "isApprovedForAnomalyTraining"
    IS_APPROVED_FOR_ALERT_TRAINING = "isApprovedForAlertTraining"
    IS_APPROVED_FOR_ANOMALY_PREDICTION = "isApprovedForAnomalyPrediction"
    IS_APPROVED_FOR_ALERT_PREDICTION = "isApprovedForAlertPrediction"
    ON_BOARDING_FLAG = "ON_BOARDING_FLAG"
    PREDICTION_CLUSTER = "prediction_cluster"
    EXISTING_CUSTOMERS = "EXISTING_CUSTOMERS"
    NEW_CUSTOMERS = "NEW_CUSTOMERS"
    MINIMUM_CUSTOMER_FOR_CLUSTER = 10000
    MINIMUM_ALERTS_FOR_CLUSTER = 10
    NO_OF_DEPENDENT_VARIABLES = 2
    DUMMY_ID_COL = "DUMMY_ID_COL"
    INCONSISTENCY_FEATURES = "INCONSISTENCY_FEATURES"
    HISTORICAL_FEATURES = "HISTORICAL_FEATURES"
    INCONSISTENCY_RULES = "Inconsistency_Behaviour"
    HISTORICAL_RULES = "Historical_Behaviour"
    EXPECTED_SINGLE = "Expected single transaction amount"
    NUMBER_OF_LINKED_CUSTOMERS = "Number of linked customers"
    UUID = "UUID"
    EMPLOYEE_FLAG = "EMPLOYEE_FLAG"
    CRS_ALERT_TYPE = "CRS"
    ALERT_PREFIX = "A_"
    PERIODIC_REVIEW = "PERIODIC REVIEW"
    FIRST_TIME_RISK = "FIRST TIME RISK"
    CHANGE_IN_RISK = "CHANGE IN RISK"
    HIGH_RISK_OCCUPATION = "HIGH_RISK_OCCUPATION"
    HIGH_RISK_BUSINESS_TYPE = "HIGH_RISK_BUSINESS_TYPE"
    CRS_RISK_LEVEL_DISTRIBUTIONS = "CRS_RISK_LEVEL_DISTRIBUTIONS"
    COUNT = "COUNT"
    ALL_POPULATION_CHALLENGER_HIGH_ACCURACY = "ALL_POPULATION_CHALLENGER_HIGH_ACCURACY"
    ALL_POPULATION_CHALLENGER_MEDIUM_ACCURACY = "ALL_POPULATION_CHALLENGER_MEDIUM_ACCURACY"
    ALL_POPULATION_CHALLENGER_LOW_ACCURACY = "ALL_POPULATION_CHALLENGER_LOW_ACCURACY"
    ALL_POPULATION_CHALLENGER_ACCURACY = "ALL_POPULATION_CHALLENGER_ACCURACY"
    REVIEWED = "REVIEWED_"
    CHALLENGER_WEIGHTED_ACCURACY = "CHALLENGER_WEIGHTED_ACCURACY"
    CHALLENGER_HIGH_ACCURACY = "CHALLENGER_HIGH_ACCURACY"
    CHALLENGER_MEDIUM_ACCURACY = "CHALLENGER_MEDIUM_ACCURACY"
    CHALLENGER_LOW_ACCURACY = "CHALLENGER_LOW_ACCURACY"
    CHALLENGER_ACCURACY = "CHALLENGER_ACCURACY"
    CHAMPION_WEIGHTED_ACCURACY = "CHAMPION_WEIGHTED_ACCURACY"
    CHAMPION_HIGH_ACCURACY = "CHAMPION_HIGH_ACCURACY"
    CHAMPION_MEDIUM_ACCURACY = "CHAMPION_MEDIUM_ACCURACY"
    CHAMPION_LOW_ACCURACY = "CHAMPION_LOW_ACCURACY"
    CHAMPION_ACCURACY = "CHAMPION_ACCURACY"
    CRS_RISK_LEVEL_DISTRIBUTIONS_DICT = {"High": 5, "Medium": 25, "Low": 70}
    CRS_RISK_LEVEL_DEFAULTS = [THRESHOLD_HIGH, THRESHOLD_MEDIUM, THRESHOLD_LOW]
    CRS_RISK_LEVEL_DEFAULTS_MAPPINGS = {THRESHOLD_HIGH: ['2', 'High', 2, 'HIGH'], THRESHOLD_MEDIUM: ['1', 'Medium', 1, 'MEDIUM'],
                                        THRESHOLD_LOW: ['0', 'Low', 0, 'LOW']}
    CLUSTER_MODEL_ID = "_modelJobId_cluster"
    COMBINED_ID = "Combined_ID"
    PREDICTION_LABEL_INTEGER = "PREDICTION_LABEL_INTEGER"
    PREDICTED = "PREDICTED"
    CHAR_STR = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    RANDOM_STRING_LENGTH = 4
    CHARS = list(CHAR_STR)
    CONSTANT_YEAR_DAYS = 365.25
    ALERTS_MODE = "CR"
    OLD_RISK_LEVEL = "OLD_RISK_LEVEL"
    CRS_FE_REFERENCE_DATE = "CREATE_DATE"
    REFERENCE_DATE = "REFERENCE_DATE"
    all_risk_levels_dict = {"nr_alert_level": ['High', 'Medium', 'Low'],
                            "er_alert_level": ['Low to High', 'Low to Medium', 'Low to Low', 'Medium to Low',
                                               'Medium to Medium',
                                               'Medium to High', 'High to Low', 'High to Medium', 'High to High']}
    default_risk_levels_dict = {"nr_alert_level": ['High'],
                                "er_alert_level": ['Low to High', 'Medium to High', 'High to High']}
    tdss_model_id = "_modelJobId"
    tdss_model_id_xgb = "_modelJobId_xgb"
    tdss_model_id_cluster = "_modelJobId_cluster"
    as_string = " AS "
    column_unwanted_str = "'>"




class CRS_TARGET_LABEL:
    target_mapping = {0: ["Non-STR", "False-Alert", "0"],
                      1: ["STR", "True-Alert", "1"]}
